import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from pyspark.sql import DataFrame, functions as F, types as T

from aq_lakehouse.spark_session import build

SILVER_TABLE = "hadoop_catalog.aq.silver.air_quality_hourly_clean"
DIM_CALENDAR_TABLE = "hadoop_catalog.aq.gold.dim_calendar_hour"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start",
        help="ISO timestamp (UTC) for start of calendar window",
    )
    parser.add_argument(
        "--end",
        help="ISO timestamp (UTC) for end of calendar window (inclusive)",
    )
    parser.add_argument(
        "--location-id",
        dest="location_ids",
        action="append",
        default=[],
        help="Optional list of location IDs to use when deriving min/max window",
    )
    parser.add_argument(
        "--mode",
        choices=["merge", "replace"],
        default="merge",
        help="replace deletes calendar rows in the requested range before merge",
    )
    return parser.parse_args()


def parse_iso_ts(value: Optional[str]) -> Optional[datetime]:
    if value is None:
        return None
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None:
        return ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def ensure_table(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.aq.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {DIM_CALENDAR_TABLE} (
          calendar_hour_key STRING,
          ts_utc TIMESTAMP,
          date_utc DATE,
          year INT,
          month INT,
          day INT,
          hour INT,
          dow INT,
          week INT,
          is_weekend BOOLEAN,
          quarter INT,
          month_name STRING,
          day_name STRING,
          created_at TIMESTAMP,
          updated_at TIMESTAMP
        ) USING iceberg
        """
    )
    spark.sql(
        f"""
        ALTER TABLE {DIM_CALENDAR_TABLE} SET TBLPROPERTIES (
          'format-version'='2',
          'write.target-file-size-bytes'='33554432',
          'write.distribution-mode'='hash'
        )
        """
    )


def current_max_calendar_hour(spark) -> Optional[datetime]:
    if not spark.catalog.tableExists(DIM_CALENDAR_TABLE):
        return None

    row = (
        spark.table(DIM_CALENDAR_TABLE)
        .agg(F.max("ts_utc").alias("max_ts"))
        .first()
    )
    if not row:
        return None

    max_ts = row["max_ts"]
    if max_ts is None:
        return None

    if max_ts.tzinfo is None:
        return max_ts.replace(tzinfo=timezone.utc)
    return max_ts.astimezone(timezone.utc)


def derive_window(
    spark,
    start_ts: Optional[datetime],
    end_ts: Optional[datetime],
    location_ids: Iterable[str],
    mode: str,
) -> tuple[datetime, datetime] | None:
    df = spark.table(SILVER_TABLE).select("location_id", "ts_utc").where(F.col("ts_utc").isNotNull())
    if location_ids:
        df = df.where(F.col("location_id").isin(list(location_ids)))

    agg = df.agg(F.min("ts_utc").alias("min_ts"), F.max("ts_utc").alias("max_ts")).first()
    if not agg or agg["min_ts"] is None or agg["max_ts"] is None:
        logging.info("No timestamps found in Silver; skipping dim_calendar_hour")
        return None

    min_ts = agg["min_ts"]
    max_ts = agg["max_ts"]

    if min_ts.tzinfo is None:
        min_ts = min_ts.replace(tzinfo=timezone.utc)
    else:
        min_ts = min_ts.astimezone(timezone.utc)
    if max_ts.tzinfo is None:
        max_ts = max_ts.replace(tzinfo=timezone.utc)
    else:
        max_ts = max_ts.astimezone(timezone.utc)

    window_start = max(min_ts, start_ts) if start_ts else min_ts
    window_end = min(max_ts, end_ts) if end_ts else max_ts

    if mode == "merge" and start_ts is None:
        latest_calendar_ts = current_max_calendar_hour(spark)
        if latest_calendar_ts is not None:
            next_hour = latest_calendar_ts + timedelta(hours=1)
            if next_hour > window_end:
                logging.info(
                    "No new dim_calendar_hour rows detected (latest existing=%s, window_end=%s)",
                    latest_calendar_ts,
                    window_end,
                )
                return None
            window_start = max(window_start, next_hour)

    if window_start > window_end:
        logging.info(
            "Window empty after filters (start=%s, end=%s)",
            window_start,
            window_end,
        )
        return None

    return window_start, window_end


def build_dataframe(spark, window_start: datetime, window_end: datetime) -> DataFrame:
    start_ts = window_start.replace(tzinfo=None)
    end_ts = window_end.replace(tzinfo=None)

    schema = T.StructType([
        T.StructField("start_ts", T.TimestampType()),
        T.StructField("end_ts", T.TimestampType()),
    ])

    window_df = spark.createDataFrame([(start_ts, end_ts)], schema)
    hours_df = window_df.select(
        F.explode(F.sequence("start_ts", "end_ts", F.expr("INTERVAL 1 HOUR"))).alias("ts_utc")
    )

    current_ts = F.current_timestamp()
    derived = hours_df.withColumn("calendar_hour_key", F.date_format("ts_utc", "yyyyMMddHH"))
    derived = derived.withColumn("date_utc", F.to_date("ts_utc"))
    derived = derived.withColumn("year", F.year("ts_utc"))
    derived = derived.withColumn("month", F.month("ts_utc"))
    derived = derived.withColumn("day", F.dayofmonth("ts_utc"))
    derived = derived.withColumn("hour", F.hour("ts_utc"))
    dow_sunday_one = F.dayofweek("ts_utc")
    dow_iso = F.when(dow_sunday_one == 1, F.lit(7)).otherwise(dow_sunday_one - 1)
    derived = derived.withColumn("dow", dow_iso.cast("int"))
    derived = derived.withColumn("week", F.weekofyear("ts_utc"))
    derived = derived.withColumn("is_weekend", F.col("dow").isin(6, 7))
    derived = derived.withColumn("quarter", F.quarter("ts_utc"))
    derived = derived.withColumn("month_name", F.date_format("ts_utc", "MMMM"))
    derived = derived.withColumn("day_name", F.date_format("ts_utc", "EEEE"))

    return (
        derived
        .select(
            "calendar_hour_key",
            "ts_utc",
            "date_utc",
            "year",
            "month",
            "day",
            "hour",
            "dow",
            "week",
            "is_weekend",
            "quarter",
            "month_name",
            "day_name",
        )
        .withColumn("created_at", current_ts)
        .withColumn("updated_at", current_ts)
    )


def delete_range(spark, start_ts: datetime, end_ts: datetime) -> None:
    start_literal = start_ts.strftime("%Y-%m-%d %H:%M:%S")
    end_literal = end_ts.strftime("%Y-%m-%d %H:%M:%S")
    logging.info(
        "Deleting dim_calendar_hour rows where ts_utc BETWEEN %s AND %s",
        start_literal,
        end_literal,
    )
    spark.sql(
        f"""
        DELETE FROM {DIM_CALENDAR_TABLE}
        WHERE ts_utc >= TIMESTAMP '{start_literal}'
          AND ts_utc <= TIMESTAMP '{end_literal}'
        """
    )


def merge_dimension(spark, df: DataFrame) -> None:
    temp_view = "tmp_dim_calendar_merge"
    df.createOrReplaceTempView(temp_view)
    try:
        spark.sql(
            f"""
            MERGE INTO {DIM_CALENDAR_TABLE} t
            USING {temp_view} s
            ON t.calendar_hour_key = s.calendar_hour_key
            WHEN MATCHED THEN UPDATE SET
              ts_utc = s.ts_utc,
              date_utc = s.date_utc,
              year = s.year,
              month = s.month,
              day = s.day,
              hour = s.hour,
              dow = s.dow,
              week = s.week,
              is_weekend = s.is_weekend,
              quarter = s.quarter,
              month_name = s.month_name,
              day_name = s.day_name,
              updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (
              calendar_hour_key,
              ts_utc,
              date_utc,
              year,
              month,
              day,
              hour,
              dow,
              week,
              is_weekend,
              quarter,
              month_name,
              day_name,
              created_at,
              updated_at
            ) VALUES (
              s.calendar_hour_key,
              s.ts_utc,
              s.date_utc,
              s.year,
              s.month,
              s.day,
              s.hour,
              s.dow,
              s.week,
              s.is_weekend,
              s.quarter,
              s.month_name,
              s.day_name,
              s.created_at,
              s.updated_at
            )
            """
        )
    finally:
        spark.catalog.dropTempView(temp_view)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    args = parse_args()

    start_ts = parse_iso_ts(args.start)
    end_ts = parse_iso_ts(args.end)

    spark = build("gold_dim_calendar_hour")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        ensure_table(spark)

        window = derive_window(spark, start_ts, end_ts, args.location_ids, args.mode)
        if window is None:
            return

        window_start, window_end = window

        df = build_dataframe(spark, window_start, window_end)
        rows = df.count()
        logging.info(
            "Preparing to upsert %d dim_calendar_hour rows between %s and %s",
            rows,
            window_start,
            window_end,
        )

        if args.mode == "replace":
            delete_range(spark, window_start, window_end)

        merge_dimension(spark, df)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
