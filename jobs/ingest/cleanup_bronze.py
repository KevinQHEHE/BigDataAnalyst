import argparse
from datetime import datetime, timedelta, timezone
from typing import Iterable, List

from pyspark.sql import Window
from pyspark.sql import functions as F, types as T

from aq_lakehouse.spark_session import build


TABLE_NAME = "hadoop_catalog.aq.raw_open_meteo_hourly"
MEASURE_COLUMNS: List[str] = [
    "aerosol_optical_depth",
    "pm2_5",
    "pm10",
    "dust",
    "nitrogen_dioxide",
    "ozone",
    "sulphur_dioxide",
    "carbon_monoxide",
    "uv_index",
    "uv_index_clear_sky",
]


def parse_date(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=None)


def collect_partitions(filtered_df) -> List[datetime.date]:
    rows = (
        filtered_df.select(F.to_date("ts").alias("partition_day"))
        .distinct()
        .collect()
    )
    return [r.partition_day for r in rows if r.partition_day is not None]


def sanitize_measurements(df):
    for col in MEASURE_COLUMNS:
        df = df.withColumn(col, F.when(F.col(col) < 0, None).otherwise(F.col(col)))
    return df


def deduplicate(df):
    window = Window.partitionBy("location_id", "ts").orderBy(F.col("ingest_ts").desc())
    return df.withColumn("_rn", F.row_number().over(window)).where(F.col("_rn") == 1).drop("_rn")


def filter_base_df(df, start_dt, end_dt, location_id):
    if start_dt:
        df = df.filter(F.col("ts") >= F.lit(start_dt))
    if end_dt:
        df = df.filter(F.col("ts") < F.lit(end_dt))
    if location_id:
        df = df.filter(F.col("location_id") == location_id)
    return df


def target_dataset(base_df, partitions: Iterable):
    if not partitions:
        return base_df.limit(0)
    schema = T.StructType([T.StructField("partition_day", T.DateType(), False)])
    part_df = base_df.sparkSession.createDataFrame([(p,) for p in partitions], schema=schema)
    return (
        base_df.join(part_df, F.to_date("ts") == F.col("partition_day"), "inner")
        .drop("partition_day")
    )


def main():
    parser = argparse.ArgumentParser(description="Clean Bronze table (dedupe + sanitize measurements).")
    parser.add_argument("--start-date", help="Inclusive start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="Inclusive end date (YYYY-MM-DD)")
    parser.add_argument("--location-id", help="Limit to location id (optional)")
    parser.add_argument("--dry-run", action="store_true", help="Analyze only, do not write back")
    args = parser.parse_args()

    start_dt = parse_date(args.start_date) if args.start_date else None
    end_dt = parse_date(args.end_date) + timedelta(days=1) if args.end_date else None

    spark = build("cleanup_open_meteo_bronze")
    base_df = spark.table(TABLE_NAME)

    filtered_view = filter_base_df(base_df, start_dt, end_dt, args.location_id)
    partitions = collect_partitions(filtered_view)

    if not partitions:
        print("[INFO] No partitions match the supplied filters; nothing to clean.")
        return

    print(f"[INFO] Target partitions: {[p.isoformat() for p in partitions]}")
    target_df = target_dataset(base_df, partitions)

    before_count = target_df.count()
    duplicate_rows = (
        target_df.groupBy("location_id", "ts")
        .count()
        .where(F.col("count") > 1)
    )
    duplicate_count = duplicate_rows.count()

    invalid_counts = target_df.select([
        F.sum(F.when(F.col(col) < 0, 1).otherwise(0)).alias(col) for col in MEASURE_COLUMNS
    ]).collect()[0]

    print(f"[INFO] Rows before cleanup: {before_count}")
    print(f"[INFO] Duplicate keys in scope: {duplicate_count}")
    if duplicate_count:
        duplicate_rows.orderBy(F.col("count").desc()).show(10, truncate=False)
    print("[INFO] Negative value counts per column:")
    for col in MEASURE_COLUMNS:
        value = invalid_counts[col] or 0
        print(f"  - {col}: {value}")

    cleaned = sanitize_measurements(target_df)
    cleaned = deduplicate(cleaned)
    after_count = cleaned.count()

    print(f"[INFO] Rows after cleanup: {after_count}")

    if args.dry_run:
        print("[INFO] Dry run enabled; no data written.")
        return

    cleaned.writeTo(TABLE_NAME).overwritePartitions()

    print("[INFO] Cleanup committed. Post-check (duplicates should be 0):")
    (
        spark.table(TABLE_NAME)
        .where(F.to_date("ts").isin(partitions))
        .groupBy("location_id", "ts")
        .count()
        .where(F.col("count") > 1)
        .show(10, truncate=False)
    )


if __name__ == "__main__":
    main()
