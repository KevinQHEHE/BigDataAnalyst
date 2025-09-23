import argparse
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterable

from pyspark.sql import DataFrame, Window, functions as F

from aq_lakehouse.spark_session import build

CLEAN_TABLE = "hadoop_catalog.aq.silver.air_quality_hourly_clean"
COMPONENT_TABLE = "hadoop_catalog.aq.silver.aq_components_hourly"
PM_WINDOW_HOURS = 24
PM_MIN_VALID_HOURS = 18
EIGHT_HOUR_WINDOW = 8
EIGHT_HOUR_MIN_VALID = 6


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="ISO timestamp (UTC) for start of computation window")
    parser.add_argument("--end", required=True, help="ISO timestamp (UTC) for end of computation window (inclusive)")
    parser.add_argument("--location-id", dest="location_ids", action="append", default=[], help="limit to one or more location IDs")
    parser.add_argument(
        "--mode",
        choices=["merge", "replace"],
        default="merge",
        help="merge keeps old rows, replace deletes rows in the target window before write",
    )
    parser.add_argument(
        "--calc-method",
        default="simple_rolling_v1",
        help="value to store in calc_method column",
    )
    return parser.parse_args()


def parse_iso_ts(value: str) -> datetime:
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    else:
        ts = ts.astimezone(timezone.utc)
    return ts


def ensure_table(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.aq")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.aq.silver")
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.aq.silver.aq_components_hourly (
          location_id STRING,
          ts_utc TIMESTAMP,
          date_utc DATE,
          pm25_24h_avg DOUBLE,
          pm10_24h_avg DOUBLE,
          o3_8h_max DOUBLE,
          co_8h_max DOUBLE,
          no2_1h_max DOUBLE,
          so2_1h_max DOUBLE,
          component_valid_flags MAP<STRING, BOOLEAN>,
          calc_method STRING,
          run_id STRING,
          computed_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (location_id, days(ts_utc))
        """
    )
    spark.sql(
        """
        ALTER TABLE hadoop_catalog.aq.silver.aq_components_hourly SET TBLPROPERTIES (
          'format-version'='2',
          'write.target-file-size-bytes'='134217728',
          'write.distribution-mode'='hash'
        )
        """
    )


def load_clean(spark, start_ts: datetime, end_ts: datetime, location_ids: Iterable[str]) -> DataFrame:
    lookback = timedelta(hours=max(PM_WINDOW_HOURS - 1, EIGHT_HOUR_WINDOW - 1))
    window_start = start_ts - lookback

    condition = (F.col("ts_utc") >= F.lit(window_start)) & (F.col("ts_utc") <= F.lit(end_ts))
    df = spark.table(CLEAN_TABLE).where(condition)
    if location_ids:
        df = df.where(F.col("location_id").isin(list(location_ids)))
    return df


def add_windows(df: DataFrame) -> DataFrame:
    df = df.withColumn("ts_epoch", F.col("ts_utc").cast("long"))

    pm_window = (
        Window.partitionBy("location_id")
        .orderBy("ts_epoch")
        .rangeBetween(-(PM_WINDOW_HOURS - 1) * 3600, 0)
    )

    eight_window = (
        Window.partitionBy("location_id")
        .orderBy("ts_epoch")
        .rangeBetween(-(EIGHT_HOUR_WINDOW - 1) * 3600, 0)
    )

    # counts of valid rows in the windows
    pm25_count = F.count(F.col("pm25")).over(pm_window)
    pm10_count = F.count(F.col("pm10")).over(pm_window)
    o3_count = F.count(F.col("o3")).over(eight_window)
    co_count = F.count(F.col("co")).over(eight_window)

    enriched = (
        df
        .withColumn("pm25_24h_avg_raw", F.avg("pm25").over(pm_window))
        .withColumn("pm10_24h_avg_raw", F.avg("pm10").over(pm_window))
        .withColumn("o3_8h_max_raw", F.max("o3").over(eight_window))
        .withColumn("co_8h_max_raw", F.max("co").over(eight_window))
        .withColumn("pm25_count", pm25_count)
        .withColumn("pm10_count", pm10_count)
        .withColumn("o3_count", o3_count)
        .withColumn("co_count", co_count)
    )

    # check hour continuity: difference between this ts and previous within location should be 3600 seconds
    lag_epoch = F.lag("ts_epoch").over(Window.partitionBy("location_id").orderBy("ts_epoch"))
    enriched = enriched.withColumn(
        "continuous_hour",
        F.when(
            lag_epoch.isNull(),
            F.lit(True)
        ).otherwise((F.col("ts_epoch") - lag_epoch) == 3600),
    )

    return enriched


def compute_components(df: DataFrame, calc_method: str, run_id: str) -> DataFrame:
    df = add_windows(df)

    pm25_value = F.when(F.col("pm25_count") >= PM_MIN_VALID_HOURS, F.col("pm25_24h_avg_raw"))
    pm10_value = F.when(F.col("pm10_count") >= PM_MIN_VALID_HOURS, F.col("pm10_24h_avg_raw"))
    o3_value = F.when(F.col("o3_count") >= EIGHT_HOUR_MIN_VALID, F.col("o3_8h_max_raw"))
    co_value = F.when(F.col("co_count") >= EIGHT_HOUR_MIN_VALID, F.col("co_8h_max_raw"))
    no2_value = F.col("no2")
    so2_value = F.col("so2")

    component_flags_entries = [
        F.lit("pm25_24h_sufficient"), (F.col("pm25_count") >= PM_MIN_VALID_HOURS),
        F.lit("pm10_24h_sufficient"), (F.col("pm10_count") >= PM_MIN_VALID_HOURS),
        F.lit("o3_8h_sufficient"), (F.col("o3_count") >= EIGHT_HOUR_MIN_VALID),
        F.lit("co_8h_sufficient"), (F.col("co_count") >= EIGHT_HOUR_MIN_VALID),
        F.lit("no2_present"), (F.col("no2").isNotNull()),
        F.lit("so2_present"), (F.col("so2").isNotNull()),
        F.lit("continuous_hour"), F.col("continuous_hour"),
    ]

    result = (
        df
        .withColumn("pm25_24h_avg", pm25_value)
        .withColumn("pm10_24h_avg", pm10_value)
        .withColumn("o3_8h_max", o3_value)
        .withColumn("co_8h_max", co_value)
        .withColumn("no2_1h_max", no2_value)
        .withColumn("so2_1h_max", so2_value)
        .withColumn("component_valid_flags", F.create_map(*component_flags_entries))
        .withColumn("calc_method", F.lit(calc_method))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("computed_at", F.current_timestamp())
        .withColumn("date_utc", F.to_date("ts_utc"))
    )

    select_cols = [
        "location_id",
        "ts_utc",
        "date_utc",
        "pm25_24h_avg",
        "pm10_24h_avg",
        "o3_8h_max",
        "co_8h_max",
        "no2_1h_max",
        "so2_1h_max",
        "component_valid_flags",
        "calc_method",
        "run_id",
        "computed_at",
    ]

    return result.select(*select_cols)


def filter_target_window(df: DataFrame, start_ts: datetime, end_ts: datetime) -> DataFrame:
    return df.where((F.col("ts_utc") >= F.lit(start_ts)) & (F.col("ts_utc") <= F.lit(end_ts)))


def delete_existing(spark, start_ts: datetime, end_ts: datetime, location_ids: Iterable[str]) -> None:
    predicates = [
        f"ts_utc >= TIMESTAMP '{start_ts.strftime('%Y-%m-%d %H:%M:%S')}'",
        f"ts_utc <= TIMESTAMP '{end_ts.strftime('%Y-%m-%d %H:%M:%S')}'",
    ]
    if location_ids:
        ids = ",".join([f"'{lid}'" for lid in location_ids])
        predicates.append(f"location_id IN ({ids})")

    condition = " AND ".join(predicates)
    logging.info("Deleting existing component rows where %s", condition)
    spark.sql(f"DELETE FROM {COMPONENT_TABLE} WHERE {condition}")


def write_to_table(spark, df: DataFrame) -> None:
    temp_view = f"staging_components_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(temp_view)
    try:
        spark.sql(
            f"""
            MERGE INTO {COMPONENT_TABLE} t
            USING {temp_view} s
            ON t.location_id = s.location_id AND t.ts_utc = s.ts_utc
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    finally:
        spark.catalog.dropTempView(temp_view)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    args = parse_args()

    start_ts = parse_iso_ts(args.start)
    end_ts = parse_iso_ts(args.end)
    if start_ts > end_ts:
        raise SystemExit("--start must be <= --end")

    spark = build("silver_components_hourly")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        ensure_table(spark)
        df_clean = load_clean(spark, start_ts, end_ts, args.location_ids)
        if df_clean.rdd.isEmpty():
            logging.info("No clean rows found for requested window; nothing to compute")
            return

        run_id = str(uuid.uuid4())
        df_components = compute_components(df_clean, args.calc_method, run_id)
        df_components = filter_target_window(df_components, start_ts, end_ts)

        if df_components.rdd.isEmpty():
            logging.info("No component rows after filtering target window; nothing to write")
            return

        if args.mode == "replace":
            delete_existing(spark, start_ts, end_ts, args.location_ids)

        write_to_table(spark, df_components)

        spark.sql(
            f"""
            SELECT location_id,
                   MIN(ts_utc) AS min_ts,
                   MAX(ts_utc) AS max_ts,
                   COUNT(*)    AS rows
            FROM {COMPONENT_TABLE}
            WHERE ts_utc BETWEEN TIMESTAMP '{start_ts.strftime('%Y-%m-%d %H:%M:%S')}'
                             AND TIMESTAMP '{end_ts.strftime('%Y-%m-%d %H:%M:%S')}'
            GROUP BY location_id
            ORDER BY location_id
            """
        ).show(truncate=False)

        print(f"RUN_ID={run_id}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
