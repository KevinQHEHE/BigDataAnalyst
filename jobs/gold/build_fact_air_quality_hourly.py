import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from pyspark.sql import DataFrame, Window, functions as F

from aq_lakehouse.spark_session import build

SILVER_CLEAN_TABLE = "hadoop_catalog.aq.silver.air_quality_hourly_clean"
SILVER_COMPONENT_TABLE = "hadoop_catalog.aq.silver.aq_components_hourly"
SILVER_INDEX_TABLE = "hadoop_catalog.aq.silver.aq_index_hourly"
DIM_LOCATION_TABLE = "hadoop_catalog.aq.gold.dim_location"
FACT_TABLE = "hadoop_catalog.aq.gold.fact_air_quality_hourly"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--start",
        help="ISO timestamp (UTC) for start of window",
    )
    parser.add_argument(
        "--end",
        help="ISO timestamp (UTC) for end of window (inclusive)",
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
        help="replace deletes rows in the requested window before merge",
    )
    parser.add_argument(
        "--quality-threshold",
        type=float,
        default=0.75,
        help="Minimum fraction of true flags in valid_flags map to mark record as validated",
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
        CREATE TABLE IF NOT EXISTS {FACT_TABLE} (
          location_key STRING,
          date_key STRING,
          time_key STRING,
          ts_utc TIMESTAMP,
          date_utc DATE,
          pm25 DOUBLE,
          pm10 DOUBLE,
          o3 DOUBLE,
          no2 DOUBLE,
          so2 DOUBLE,
          co DOUBLE,
          aod DOUBLE,
          uv_index DOUBLE,
          uv_index_clear_sky DOUBLE,
          pm25_24h_avg DOUBLE,
          pm10_24h_avg DOUBLE,
          pm25_nowcast DOUBLE,
          o3_8h_max DOUBLE,
          no2_1h_max DOUBLE,
          so2_1h_max DOUBLE,
          co_8h_avg DOUBLE,
          aqi INT,
          category STRING,
          dominant_pollutant STRING,
          is_validated BOOLEAN,
          quality_flag STRING,
          data_source STRING,
          run_id STRING,
          ingested_at TIMESTAMP,
          computed_at TIMESTAMP,
          component_calc_method STRING,
          index_calc_method STRING,
          created_at TIMESTAMP,
          updated_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (location_key, days(ts_utc))
        """
    )
    spark.sql(
        f"""
        ALTER TABLE {FACT_TABLE} SET TBLPROPERTIES (
          'format-version'='2',
          'write.target-file-size-bytes'='33554432',
          'write.distribution-mode'='hash'
        )
        """
    )

    if spark.catalog.tableExists(FACT_TABLE):
        field_names = {field.name for field in spark.table(FACT_TABLE).schema.fields}
        if "calendar_hour_key" in field_names:
            spark.sql(f"ALTER TABLE {FACT_TABLE} DROP COLUMN calendar_hour_key")
            field_names.remove("calendar_hour_key")
        for column_name, column_type in [("date_key", "STRING"), ("time_key", "STRING"), ("pm10_24h_avg", "DOUBLE")]:
            if column_name not in field_names:
                spark.sql(f"ALTER TABLE {FACT_TABLE} ADD COLUMN {column_name} {column_type}")


def current_max_fact_ts(spark) -> Optional[datetime]:
    if not spark.catalog.tableExists(FACT_TABLE):
        return None
    row = spark.table(FACT_TABLE).agg(F.max("ts_utc").alias("max_ts")).first()
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
    clean_df = spark.table(SILVER_CLEAN_TABLE).select("location_id", "ts_utc").where(F.col("ts_utc").isNotNull())
    if location_ids:
        clean_df = clean_df.where(F.col("location_id").isin(list(location_ids)))

    agg = clean_df.agg(F.min("ts_utc").alias("min_ts"), F.max("ts_utc").alias("max_ts")).first()
    if not agg or agg["min_ts"] is None or agg["max_ts"] is None:
        logging.info("No source rows found for fact_air_quality_hourly")
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
        latest_fact_ts = current_max_fact_ts(spark)
        if latest_fact_ts is not None:
            next_hour = latest_fact_ts + timedelta(hours=1)
            if next_hour > window_end:
                logging.info(
                    "No new fact_air_quality_hourly rows detected (latest existing=%s, window_end=%s)",
                    latest_fact_ts,
                    window_end,
                )
                return None
            window_start = max(window_start, next_hour)

    if window_start > window_end:
        logging.info(
            "fact_air_quality_hourly window empty after filters (start=%s, end=%s)",
            window_start,
            window_end,
        )
        return None

    return window_start, window_end


def load_source_frames(spark, window_start: datetime, window_end: datetime, location_ids: Iterable[str]) -> tuple[DataFrame, DataFrame, DataFrame]:
    cond = (F.col("ts_utc") >= F.lit(window_start)) & (F.col("ts_utc") <= F.lit(window_end))

    clean = spark.table(SILVER_CLEAN_TABLE).where(cond)
    components = spark.table(SILVER_COMPONENT_TABLE).where(cond)
    index = spark.table(SILVER_INDEX_TABLE).where(cond)

    if location_ids:
        ids = list(location_ids)
        clean = clean.where(F.col("location_id").isin(ids))
        components = components.where(F.col("location_id").isin(ids))
        index = index.where(F.col("location_id").isin(ids))

    return clean, components, index


def compute_quality(valid_flags: F.Column, threshold: float) -> tuple[F.Column, F.Column]:
    safe_map = F.coalesce(valid_flags, F.map_from_arrays(F.array(), F.array()))
    values = F.map_values(safe_map)
    total = F.size(values)
    true_count = F.aggregate(values, F.lit(0), lambda acc, x: acc + F.when(x.cast("boolean"), 1).otherwise(0))
    fraction = F.when(total > 0, true_count.cast("double") / total).otherwise(F.lit(0.0))
    is_validated = fraction >= F.lit(threshold)
    quality_flag = F.when(is_validated, F.lit("validated")).otherwise(F.lit("needs_review"))
    return is_validated, quality_flag


def compute_co_8h_avg(clean: DataFrame) -> DataFrame:
    window = (
        Window.partitionBy("location_id")
        .orderBy(F.col("ts_utc").cast("long"))
        .rangeBetween(-7 * 3600, 0)
    )
    return clean.withColumn("co_8h_avg", F.avg("co").over(window))


def build_dataframe(
    spark,
    window_start: datetime,
    window_end: datetime,
    location_ids: Iterable[str],
    quality_threshold: float,
) -> DataFrame:
    clean, components, index = load_source_frames(spark, window_start, window_end, location_ids)

    if clean.rdd.isEmpty():  # type: ignore[attr-defined]
        empty_schema = spark.table(FACT_TABLE).limit(0).schema
        return spark.createDataFrame([], schema=empty_schema)

    clean_with_avg = compute_co_8h_avg(clean)

    comp_select = (
        components
        .select(
            "location_id",
            "ts_utc",
            "pm25_24h_avg",
            "pm10_24h_avg",
            "o3_8h_max",
            "co_8h_max",
            "no2_1h_max",
            "so2_1h_max",
            "component_valid_flags",
            F.col("calc_method").alias("component_calc_method"),
            F.col("run_id").alias("component_run_id"),
            F.col("computed_at").alias("component_computed_at"),
        )
    )

    index_select = (
        index
        .select(
            "location_id",
            "ts_utc",
            "aqi",
            "category",
            "dominant_pollutant",
            "aqi_pm25",
            "aqi_pm10",
            "aqi_o3",
            "aqi_no2",
            "aqi_so2",
            "aqi_co",
            F.col("calc_method").alias("index_calc_method"),
            F.col("run_id").alias("index_run_id"),
            F.col("computed_at").alias("index_computed_at"),
        )
    )

    fact_stage = (
        clean_with_avg.alias("c")
        .join(comp_select.alias("cmp"), ["location_id", "ts_utc"], "left")
        .join(index_select.alias("idx"), ["location_id", "ts_utc"], "left")
    )

    location_dim = spark.table(DIM_LOCATION_TABLE).select("location_key").alias("loc")
    fact_stage = fact_stage.join(location_dim, F.col("c.location_id") == F.col("loc.location_key"), "inner")

    is_validated, quality_flag = compute_quality(F.col("c.valid_flags"), quality_threshold)

    computed_at = F.greatest(
        F.col("c.ingested_at"),
        F.col("cmp.component_computed_at"),
        F.col("idx.index_computed_at"),
        F.current_timestamp(),
    )

    pm25_nowcast = F.lit(None).cast("double")

    result = (
        fact_stage
        .select(
            F.col("loc.location_key").alias("location_key"),
            F.date_format(F.col("c.date_utc"), "yyyyMMdd").alias("date_key"),
            F.date_format(F.col("c.ts_utc"), "HH").alias("time_key"),
            F.col("c.ts_utc"),
            F.col("c.date_utc"),
            F.col("c.pm25"),
            F.col("c.pm10"),
            F.col("c.o3"),
            F.col("c.no2"),
            F.col("c.so2"),
            F.col("c.co"),
            F.col("c.aod"),
            F.col("c.uv_index"),
            F.col("c.uv_index_clear_sky"),
            F.col("cmp.pm25_24h_avg"),
            F.col("cmp.pm10_24h_avg"),
            pm25_nowcast.alias("pm25_nowcast"),
            F.col("cmp.o3_8h_max"),
            F.col("cmp.no2_1h_max"),
            F.col("cmp.so2_1h_max"),
            F.coalesce(F.col("cmp.co_8h_max"), F.col("c.co_8h_avg")).alias("co_8h_avg"),
            F.col("idx.aqi"),
            F.col("idx.category"),
            F.col("idx.dominant_pollutant"),
            is_validated.alias("is_validated"),
            quality_flag.alias("quality_flag"),
            F.col("c.source").alias("data_source"),
            F.coalesce(F.col("idx.index_run_id"), F.col("cmp.component_run_id"), F.col("c.run_id")).alias("run_id"),
            F.col("c.ingested_at"),
            computed_at.alias("computed_at"),
            F.col("cmp.component_calc_method"),
            F.col("idx.index_calc_method"),
            F.current_timestamp().alias("created_at"),
            F.current_timestamp().alias("updated_at"),
        )
    )

    return result.dropDuplicates(["location_key", "date_key", "time_key"]).repartition("location_key", "date_key")


def upsert_fact(spark, df: DataFrame) -> None:
    partition_cols = ["location_key", "date_key"]
    key_cols = ["location_key", "date_key", "time_key"]

    affected_partitions = df.select(*partition_cols).distinct()

    if spark.catalog.tableExists(FACT_TABLE):
        existing = spark.table(FACT_TABLE).join(affected_partitions, partition_cols, "inner")
    else:
        existing = None

    new_tagged = df.withColumn("_priority", F.lit(0))

    if existing is not None and existing.take(1):
        existing_tagged = existing.withColumn("_priority", F.lit(1))
        combined = new_tagged.unionByName(existing_tagged)
    else:
        combined = new_tagged

    window = (
        Window.partitionBy(*key_cols)
        .orderBy(
            F.col("_priority"),
            F.col("computed_at").desc_nulls_last(),
            F.col("ingested_at").desc_nulls_last(),
        )
    )

    deduped = (
        combined
        .withColumn("_rn", F.row_number().over(window))
        .where(F.col("_rn") == 1)
        .drop("_rn", "_priority")
        .repartition("location_key", "date_key")
    )

    deduped.writeTo(FACT_TABLE).overwritePartitions()


CHUNK_DAYS = 7


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    args = parse_args()

    start_ts = parse_iso_ts(args.start)
    end_ts = parse_iso_ts(args.end)

    spark = build("gold_fact_air_quality_hourly")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        ensure_table(spark)

        window = derive_window(spark, start_ts, end_ts, args.location_ids, args.mode)
        if window is None:
            return

        window_start, window_end = window

        chunk_start = window_start
        while chunk_start <= window_end:
            chunk_end = min(chunk_start + timedelta(days=CHUNK_DAYS) - timedelta(hours=1), window_end)
            logging.info(
                "Processing chunk %s -> %s",
                chunk_start,
                chunk_end,
            )

            df = build_dataframe(
                spark,
                chunk_start,
                chunk_end,
                args.location_ids,
                args.quality_threshold,
            ).cache()

            sample = df.take(1)
            if not sample:
                logging.info("Chunk empty; skipping")
                df.unpersist()
                chunk_start = chunk_end + timedelta(hours=1)
                continue

            try:
                rows = df.count()
                logging.info(
                    "Preparing to upsert %d fact_air_quality_hourly rows between %s and %s",
                    rows,
                    chunk_start,
                    chunk_end,
                )

                upsert_fact(spark, df)
            finally:
                df.unpersist()

            chunk_start = chunk_end + timedelta(hours=1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
