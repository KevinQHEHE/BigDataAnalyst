import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from pyspark.sql import DataFrame, Window, functions as F

from aq_lakehouse.spark_session import build

FACT_HOURLY_TABLE = "hadoop_catalog.aq.gold.fact_air_quality_hourly"
DIM_LOCATION_TABLE = "hadoop_catalog.aq.gold.dim_location"
FACT_DAILY_TABLE = "hadoop_catalog.aq.gold.fact_city_daily"
CHUNK_DAYS = 14


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start",
        help="ISO timestamp (UTC) for start of aggregation window",
    )
    parser.add_argument(
        "--end",
        help="ISO timestamp (UTC) for end of aggregation window",
    )
    parser.add_argument(
        "--location-id",
        dest="location_ids",
        action="append",
        default=[],
        help="Optional list of location IDs to process",
    )
    parser.add_argument(
        "--mode",
        choices=["merge", "replace"],
        default="merge",
        help="replace deletes rows for the requested window before merge",
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
        CREATE TABLE IF NOT EXISTS {FACT_DAILY_TABLE} (
          location_key STRING,
          date_utc DATE,
          pm25_daily_avg DOUBLE,
          pm25_daily_max DOUBLE,
          pm25_daily_min DOUBLE,
          pm10_daily_avg DOUBLE,
          pm10_daily_max DOUBLE,
          pm10_daily_min DOUBLE,
          o3_daily_avg DOUBLE,
          o3_daily_max DOUBLE,
          no2_daily_avg DOUBLE,
          no2_daily_max DOUBLE,
          so2_daily_avg DOUBLE,
          so2_daily_max DOUBLE,
          co_daily_avg DOUBLE,
          co_daily_max DOUBLE,
          aod_daily_avg DOUBLE,
          uv_index_daily_avg DOUBLE,
          uv_index_daily_max DOUBLE,
          uv_index_clear_sky_daily_avg DOUBLE,
          uv_index_clear_sky_daily_max DOUBLE,
          aqi_daily_avg DOUBLE,
          aqi_daily_max INT,
          dominant_pollutant_daily STRING,
          hours_measured INT,
          total_hours INT,
          data_completeness DOUBLE,
          pm25_24h_avg DOUBLE,
          pm10_24h_avg DOUBLE,
          co_8h_avg DOUBLE,
          notes STRING,
          computed_at TIMESTAMP,
          created_at TIMESTAMP,
          updated_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (location_key, date_utc)
        """
    )
    spark.sql(
        f"""
        ALTER TABLE {FACT_DAILY_TABLE} SET TBLPROPERTIES (
          'format-version'='2',
          'write.target-file-size-bytes'='33554432',
          'write.distribution-mode'='hash'
        )
        """
    )


def current_max_daily_date(spark) -> Optional[datetime]:
    if not spark.catalog.tableExists(FACT_DAILY_TABLE):
        return None
    row = spark.table(FACT_DAILY_TABLE).agg(F.max("date_utc").alias("max_date")).first()
    if not row:
        return None
    max_date = row["max_date"]
    if max_date is None:
        return None
    return datetime.combine(max_date, datetime.min.time(), tzinfo=timezone.utc)


def derive_window(
    spark,
    start_ts: Optional[datetime],
    end_ts: Optional[datetime],
    location_ids: Iterable[str],
    mode: str,
) -> tuple[datetime, datetime] | None:
    if not spark.catalog.tableExists(FACT_HOURLY_TABLE):
        logging.info("Hourly fact table not found; nothing to aggregate")
        return None

    hourly = spark.table(FACT_HOURLY_TABLE).select("location_key", "ts_utc").where(F.col("ts_utc").isNotNull())
    if location_ids:
        hourly = hourly.where(F.col("location_key").isin(list(location_ids)))

    agg = hourly.agg(F.min("ts_utc").alias("min_ts"), F.max("ts_utc").alias("max_ts")).first()
    if not agg or agg["min_ts"] is None or agg["max_ts"] is None:
        logging.info("No hourly fact rows available for daily aggregation")
        return None

    min_ts = agg["min_ts"].astimezone(timezone.utc) if agg["min_ts"].tzinfo else agg["min_ts"].replace(tzinfo=timezone.utc)
    max_ts = agg["max_ts"].astimezone(timezone.utc) if agg["max_ts"].tzinfo else agg["max_ts"].replace(tzinfo=timezone.utc)

    window_start = max(min_ts, start_ts) if start_ts else min_ts
    window_end = min(max_ts, end_ts) if end_ts else max_ts

    if mode == "merge" and start_ts is None:
        latest_daily = current_max_daily_date(spark)
        if latest_daily is not None:
            next_day = latest_daily + timedelta(days=1)
            if next_day > window_end:
                logging.info(
                    "No new fact_city_daily rows detected (latest existing=%s, window_end=%s)",
                    latest_daily,
                    window_end,
                )
                return None
            window_start = max(window_start, next_day)

    if window_start > window_end:
        logging.info(
            "fact_city_daily window empty after filters (start=%s, end=%s)",
            window_start,
            window_end,
        )
        return None

    # Align to full-day boundaries
    start_aligned = window_start.replace(hour=0, minute=0, second=0, microsecond=0)
    end_aligned = window_end.replace(hour=23, minute=59, second=59, microsecond=0)
    return start_aligned, end_aligned


def load_hourly(spark, start_ts: datetime, end_ts: datetime, location_ids: Iterable[str]) -> DataFrame:
    cond = (F.col("ts_utc") >= F.lit(start_ts)) & (F.col("ts_utc") <= F.lit(end_ts))
    df = spark.table(FACT_HOURLY_TABLE).where(cond)
    if location_ids:
        df = df.where(F.col("location_key").isin(list(location_ids)))

    required_double_cols = [
        "pm25_24h_avg",
        "pm10_24h_avg",
        "co_8h_avg",
    ]
    for col_name in required_double_cols:
        if col_name not in df.columns:
            df = df.withColumn(col_name, F.lit(None).cast("double"))
    return df


def aggregate_daily(df: DataFrame) -> DataFrame:
    if df.rdd.isEmpty():  # type: ignore[attr-defined]
        return df.limit(0)

    dominant_struct = F.max(F.struct(F.col("aqi"), F.col("dominant_pollutant"))).alias("dominant_struct")

    aggregated = (
        df.groupBy("location_key", "date_utc")
        .agg(
            F.avg("pm25").alias("pm25_daily_avg"),
            F.max("pm25").alias("pm25_daily_max"),
            F.min("pm25").alias("pm25_daily_min"),
            F.avg("pm10").alias("pm10_daily_avg"),
            F.max("pm10").alias("pm10_daily_max"),
            F.min("pm10").alias("pm10_daily_min"),
            F.avg("o3").alias("o3_daily_avg"),
            F.max("o3").alias("o3_daily_max"),
            F.avg("no2").alias("no2_daily_avg"),
            F.max("no2").alias("no2_daily_max"),
            F.avg("so2").alias("so2_daily_avg"),
            F.max("so2").alias("so2_daily_max"),
            F.avg("co").alias("co_daily_avg"),
            F.max("co").alias("co_daily_max"),
            F.avg("aod").alias("aod_daily_avg"),
            F.avg("uv_index").alias("uv_index_daily_avg"),
            F.max("uv_index").alias("uv_index_daily_max"),
            F.avg("uv_index_clear_sky").alias("uv_index_clear_sky_daily_avg"),
            F.max("uv_index_clear_sky").alias("uv_index_clear_sky_daily_max"),
            F.avg("aqi").alias("aqi_daily_avg"),
            F.max("aqi").cast("int").alias("aqi_daily_max"),
            dominant_struct,
            F.count("calendar_hour_key").alias("hours_measured"),
            F.lit(24).alias("total_hours"),
            F.avg("pm25_24h_avg").alias("pm25_24h_avg"),
            F.avg("pm10_24h_avg").alias("pm10_24h_avg"),
            F.avg("co_8h_avg").alias("co_8h_avg"),
        )
    )

    result = (
        aggregated
        .withColumn("dominant_pollutant_daily", F.col("dominant_struct")["dominant_pollutant"])
        .withColumn("data_completeness", F.col("hours_measured") / F.col("total_hours"))
        .drop("dominant_struct")
        .withColumn("notes", F.lit(None).cast("string"))
        .withColumn("computed_at", F.current_timestamp())
        .withColumn("created_at", F.current_timestamp())
        .withColumn("updated_at", F.current_timestamp())
    )

    return result


def delete_range(spark, start_ts: datetime, end_ts: datetime, location_ids: Iterable[str]) -> None:
    start_date = start_ts.date()
    end_date = end_ts.date()
    predicates = [
        f"date_utc >= DATE '{start_date.isoformat()}'",
        f"date_utc <= DATE '{end_date.isoformat()}'",
    ]
    if location_ids:
        ids = ",".join([f"'{lid}'" for lid in location_ids])
        predicates.append(f"location_key IN ({ids})")

    condition = " AND ".join(predicates)
    logging.info("Deleting existing fact_city_daily rows where %s", condition)
    spark.sql(f"DELETE FROM {FACT_DAILY_TABLE} WHERE {condition}")


def upsert_fact(spark, df: DataFrame) -> None:
    partition_cols = ["location_key", "date_utc"]

    affected_partitions = df.select(*partition_cols).distinct()

    if spark.catalog.tableExists(FACT_DAILY_TABLE):
        existing = spark.table(FACT_DAILY_TABLE).join(affected_partitions, partition_cols, "inner")
    else:
        existing = None

    new_tagged = df.withColumn("_priority", F.lit(0))

    if existing is not None and existing.take(1):
        existing_tagged = existing.withColumn("_priority", F.lit(1))
        combined = new_tagged.unionByName(existing_tagged)
    else:
        combined = new_tagged

    window = (
        Window.partitionBy(*partition_cols)
        .orderBy(
            F.col("_priority"),
            F.col("computed_at").desc_nulls_last(),
            F.col("updated_at").desc_nulls_last(),
        )
    )

    deduped = (
        combined
        .withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop("_rn", "_priority")
        .repartition(*partition_cols)
    )

    deduped.writeTo(FACT_DAILY_TABLE).overwritePartitions()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    args = parse_args()

    start_ts = parse_iso_ts(args.start)
    end_ts = parse_iso_ts(args.end)

    spark = build("gold_fact_city_daily")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        ensure_table(spark)

        window = derive_window(spark, start_ts, end_ts, args.location_ids, args.mode)
        if window is None:
            return

        window_start, window_end = window

        chunk_start = window_start
        while chunk_start <= window_end:
            chunk_end = min(
                chunk_start + timedelta(days=CHUNK_DAYS) - timedelta(seconds=1),
                window_end,
            )
            logging.info("Processing daily chunk %s -> %s", chunk_start, chunk_end)

            hourly_chunk = load_hourly(spark, chunk_start, chunk_end, args.location_ids).cache()
            try:
                if not hourly_chunk.take(1):
                    logging.info("Hourly fact empty for chunk; skipping")
                    chunk_start = chunk_end + timedelta(seconds=1)
                    continue

                daily_chunk = aggregate_daily(hourly_chunk).cache()
            finally:
                hourly_chunk.unpersist()

            if not daily_chunk.take(1):
                logging.info("Aggregated daily chunk empty; skipping")
                daily_chunk.unpersist()
                chunk_start = chunk_end + timedelta(seconds=1)
                continue

            try:
                rows = daily_chunk.count()
                logging.info(
                    "Preparing to upsert %d fact_city_daily rows between %s and %s",
                    rows,
                    chunk_start.date(),
                    chunk_end.date(),
                )

                if args.mode == "replace":
                    delete_range(spark, chunk_start, chunk_end, args.location_ids)

                upsert_fact(spark, daily_chunk)
            finally:
                daily_chunk.unpersist()

            chunk_start = chunk_end + timedelta(seconds=1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
