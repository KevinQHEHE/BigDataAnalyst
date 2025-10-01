import argparse
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

from pyspark.sql import DataFrame, functions as F, types as T

from aq_lakehouse.spark_session import build

SILVER_TABLE = "hadoop_catalog.aq.silver.air_quality_hourly_clean"
DIM_DATE_TABLE = "hadoop_catalog.aq.gold.dim_calendar_date"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", help="ISO timestamp (UTC) for start of calendar window")
    parser.add_argument("--end", help="ISO timestamp (UTC) for end of calendar window (inclusive)")
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
        help="replace recomputes dates in the requested window regardless of existing rows",
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
        CREATE TABLE IF NOT EXISTS {DIM_DATE_TABLE} (
          date_key STRING,
          date_utc DATE,
          year INT,
          month INT,
          day INT,
          dow INT,
          week INT,
          is_weekend BOOLEAN,
          quarter INT,
          month_name STRING,
          day_name STRING,
          created_at TIMESTAMP,
          updated_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (years(date_utc))
        """
    )
    spark.sql(
        f"""
        ALTER TABLE {DIM_DATE_TABLE} SET TBLPROPERTIES (
          'format-version'='2',
          'write.target-file-size-bytes'='33554432',
          'write.distribution-mode'='hash'
        )
        """
    )


def current_max_date_start(spark) -> Optional[datetime]:
    if not spark.catalog.tableExists(DIM_DATE_TABLE):
        return None
    row = spark.table(DIM_DATE_TABLE).agg(F.max("date_utc").alias("max_date")).first()
    if not row or row["max_date"] is None:
        return None
    return datetime.combine(row["max_date"], datetime.min.time(), tzinfo=timezone.utc)


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
        logging.info("No timestamps found in Silver; skipping calendar date build")
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
        latest_start = current_max_date_start(spark)
        if latest_start is not None:
            next_day = latest_start + timedelta(days=1)
            if next_day > window_end:
                logging.info(
                    "No new calendar dates detected (latest existing=%s, window_end=%s)",
                    latest_start,
                    window_end,
                )
                return None
            window_start = max(window_start, next_day)

    if window_start > window_end:
        logging.info(
            "Calendar date window empty after filters (start=%s, end=%s)",
            window_start,
            window_end,
        )
        return None

    return window_start, window_end


def build_days_dataframe(spark, window_start: datetime, window_end: datetime) -> DataFrame:
    start_date = window_start.date()
    end_date = window_end.date()

    schema = T.StructType([
        T.StructField("start_date", T.DateType()),
        T.StructField("end_date", T.DateType()),
    ])

    window_df = spark.createDataFrame([(start_date, end_date)], schema)
    return window_df.select(
        F.explode(F.sequence("start_date", "end_date", F.expr("INTERVAL 1 DAY"))).alias("date_utc")
    )


def build_date_dimension(days_df: DataFrame) -> DataFrame:
    current_ts = F.current_timestamp()

    dow_sunday_one = F.dayofweek("date_utc")
    dow_iso = F.when(dow_sunday_one == 1, F.lit(7)).otherwise(dow_sunday_one - 1)

    return (
        days_df
        .withColumn("date_key", F.date_format("date_utc", "yyyyMMdd"))
        .withColumn("year", F.year("date_utc"))
        .withColumn("month", F.month("date_utc"))
        .withColumn("day", F.dayofmonth("date_utc"))
        .withColumn("dow", dow_iso.cast("int"))
        .withColumn("week", F.weekofyear("date_utc"))
        .withColumn("is_weekend", F.col("dow").isin(6, 7))
        .withColumn("quarter", F.quarter("date_utc"))
        .withColumn("month_name", F.date_format("date_utc", "MMMM"))
        .withColumn("day_name", F.date_format("date_utc", "EEEE"))
        .withColumn("created_at", current_ts)
        .withColumn("updated_at", current_ts)
        .select(
            "date_key",
            "date_utc",
            "year",
            "month",
            "day",
            "dow",
            "week",
            "is_weekend",
            "quarter",
            "month_name",
            "day_name",
            "created_at",
            "updated_at",
        )
    )


def upsert_dates(spark, df: DataFrame) -> None:
    df = df.cache()
    try:
        if not df.take(1):
            logging.info("No calendar dates to write")
            return

        rows = df.count()
        df.writeTo(DIM_DATE_TABLE).overwritePartitions()
        logging.info("Upserted %d calendar date rows", rows)
    finally:
        df.unpersist()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    args = parse_args()

    start_ts = parse_iso_ts(args.start)
    end_ts = parse_iso_ts(args.end)

    spark = build("gold_dim_calendar_date")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        ensure_table(spark)

        window = derive_window(spark, start_ts, end_ts, args.location_ids, args.mode)
        if window is None:
            return

        window_start, window_end = window
        logging.info("Building calendar dates between %s and %s", window_start.date(), window_end.date())

        days_df = build_days_dataframe(spark, window_start, window_end)
        date_df = build_date_dimension(days_df)
        upsert_dates(spark, date_df)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
