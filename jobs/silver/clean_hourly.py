import argparse
import logging
import uuid
from datetime import datetime, timezone
from typing import Iterable

from pyspark.sql import DataFrame, functions as F, types as T

from aq_lakehouse.spark_session import build

BRONZE_TABLE = "hadoop_catalog.aq.raw_open_meteo_hourly"
SILVER_TABLE = "hadoop_catalog.aq.silver.air_quality_hourly_clean"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", required=True, help="ISO timestamp (UTC) for start of window")
    parser.add_argument("--end", required=True, help="ISO timestamp (UTC) for end of window (inclusive)")
    parser.add_argument("--location-id", dest="location_ids", action="append", default=[], help="filter to one or more location_id values")
    parser.add_argument(
        "--mode",
        choices=["merge", "replace"],
        default="merge",
        help="merge keeps history, replace deletes existing rows for the window before write",
    )
    parser.add_argument("--notes", help="optional note to attach to all rows written", default=None)
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
        CREATE TABLE IF NOT EXISTS hadoop_catalog.aq.silver.air_quality_hourly_clean (
          location_id STRING,
          latitude DOUBLE,
          longitude DOUBLE,
          ts_utc TIMESTAMP,
          date_utc DATE,
          aod DOUBLE,
          pm25 DOUBLE,
          pm10 DOUBLE,
          dust DOUBLE,
          no2 DOUBLE,
          o3 DOUBLE,
          so2 DOUBLE,
          co DOUBLE,
          uv_index DOUBLE,
          uv_index_clear_sky DOUBLE,
          source STRING,
          bronze_run_id STRING,
          bronze_ingested_at TIMESTAMP,
          run_id STRING,
          ingested_at TIMESTAMP,
          valid_flags MAP<STRING, BOOLEAN>,
          notes STRING
        ) USING iceberg
        PARTITIONED BY (location_id, days(ts_utc))
        """
    )
    spark.sql(
        """
        ALTER TABLE hadoop_catalog.aq.silver.air_quality_hourly_clean SET TBLPROPERTIES (
          'format-version'='2',
          'write.target-file-size-bytes'='134217728',
          'write.distribution-mode'='hash'
        )
        """
    )


def load_bronze(spark, start_ts: datetime, end_ts: datetime, location_ids: Iterable[str]) -> DataFrame:
    condition = (
        (F.col("ts") >= F.lit(start_ts))
        & (F.col("ts") <= F.lit(end_ts))
    )
    df = spark.table(BRONZE_TABLE).where(condition)
    if location_ids:
        df = df.where(F.col("location_id").isin(list(location_ids)))
    return df


def build_valid_flags(df: DataFrame) -> DataFrame:
    checks = {
        "aod": "aod_nonneg",
        "pm25": "pm25_nonneg",
        "pm10": "pm10_nonneg",
        "dust": "dust_nonneg",
        "no2": "no2_nonneg",
        "o3": "o3_nonneg",
        "so2": "so2_nonneg",
        "co": "co_nonneg",
        "uv_index": "uv_index_nonneg",
        "uv_index_clear_sky": "uv_index_clear_sky_nonneg",
    }

    entries: list = []
    for col_name, flag_name in checks.items():
        valid_expr = F.col(col_name).isNotNull() & (F.col(col_name) >= 0)
        entries.extend([F.lit(flag_name), valid_expr])

    entries.extend([
        F.lit("ts_present"), F.col("ts_utc").isNotNull(),
        F.lit("latitude_present"), F.col("latitude").isNotNull(),
        F.lit("longitude_present"), F.col("longitude").isNotNull(),
    ])

    return df.withColumn("valid_flags", F.create_map(*entries))


def transform(df: DataFrame, notes: str | None, run_id: str) -> DataFrame:
    renamed = df.select(
        "location_id",
        "latitude",
        "longitude",
        F.to_utc_timestamp("ts", "UTC").alias("ts_utc"),
        F.col("aerosol_optical_depth").alias("aod"),
        F.col("pm2_5").alias("pm25"),
        "pm10",
        "dust",
        F.col("nitrogen_dioxide").alias("no2"),
        F.col("ozone").alias("o3"),
        F.col("sulphur_dioxide").alias("so2"),
        F.col("carbon_monoxide").alias("co"),
        "uv_index",
        "uv_index_clear_sky",
        "source",
        F.col("run_id").alias("bronze_run_id"),
        F.col("ingested_at").alias("bronze_ingested_at"),
    )

    cleaned = (
        renamed
        .withColumn("date_utc", F.to_date("ts_utc"))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("notes", F.lit(notes).cast(T.StringType()))
    )

    cleaned = build_valid_flags(cleaned)

    cols_order = [
        "location_id",
        "latitude",
        "longitude",
        "ts_utc",
        "date_utc",
        "aod",
        "pm25",
        "pm10",
        "dust",
        "no2",
        "o3",
        "so2",
        "co",
        "uv_index",
        "uv_index_clear_sky",
        "source",
        "bronze_run_id",
        "bronze_ingested_at",
        "run_id",
        "ingested_at",
        "valid_flags",
        "notes",
    ]
    return cleaned.select(*cols_order)


def delete_existing(spark, start_ts: datetime, end_ts: datetime, location_ids: Iterable[str]) -> None:
    predicates = [
        f"ts_utc >= TIMESTAMP '{start_ts.strftime('%Y-%m-%d %H:%M:%S')}'",
        f"ts_utc <= TIMESTAMP '{end_ts.strftime('%Y-%m-%d %H:%M:%S')}'",
    ]
    if location_ids:
        ids = ",".join([f"'{lid}'" for lid in location_ids])
        predicates.append(f"location_id IN ({ids})")

    condition = " AND ".join(predicates)
    logging.info("Deleting existing Silver rows where %s", condition)
    spark.sql(f"DELETE FROM {SILVER_TABLE} WHERE {condition}")


def write_to_table(spark, df: DataFrame) -> None:
    temp_view = f"staging_clean_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(temp_view)
    try:
        spark.sql(
            f"""
            MERGE INTO {SILVER_TABLE} t
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

    spark = build("silver_air_quality_clean")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        ensure_table(spark)
        df_bronze = load_bronze(spark, start_ts, end_ts, args.location_ids)

        if df_bronze.rdd.isEmpty():
            logging.info("No Bronze rows found for requested window; nothing to write")
            return

        run_id = str(uuid.uuid4())
        df_clean = transform(df_bronze, args.notes, run_id)

        if args.mode == "replace":
            delete_existing(spark, start_ts, end_ts, args.location_ids)

        write_to_table(spark, df_clean)

        spark.sql(
            f"""
            SELECT location_id,
                   MIN(ts_utc) AS min_ts,
                   MAX(ts_utc) AS max_ts,
                   COUNT(*)    AS rows
            FROM {SILVER_TABLE}
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
