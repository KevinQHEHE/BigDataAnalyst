import argparse
import logging
from typing import Optional

from pyspark.sql import DataFrame, functions as F, types as T

from aq_lakehouse.spark_session import build

DIM_TIME_TABLE = "hadoop_catalog.aq.gold.dim_calendar_time"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode",
        choices=["merge", "replace"],
        default="merge",
        help="replace rewrites the entire time dimension",
    )
    return parser.parse_args()


def ensure_table(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.aq.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {DIM_TIME_TABLE} (
          time_key STRING,
          hour INT,
          hour_label STRING,
          is_am BOOLEAN,
          day_part STRING,
          created_at TIMESTAMP,
          updated_at TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (hour)
        """
    )
    spark.sql(
        f"""
        ALTER TABLE {DIM_TIME_TABLE} SET TBLPROPERTIES (
          'format-version'='2',
          'write.target-file-size-bytes'='33554432',
          'write.distribution-mode'='hash'
        )
        """
    )


def build_time_dimension(spark) -> DataFrame:
    hours = [(i,) for i in range(24)]
    schema = T.StructType([T.StructField("hour", T.IntegerType(), False)])
    base = spark.createDataFrame(hours, schema)

    current_ts = F.current_timestamp()

    day_part = (
        F.when(F.col("hour") < 6, F.lit("Night"))
        .when(F.col("hour") < 12, F.lit("Morning"))
        .when(F.col("hour") < 18, F.lit("Afternoon"))
        .otherwise(F.lit("Evening"))
    )

    return (
        base
        .withColumn("time_key", F.format_string("%02d", F.col("hour")))
        .withColumn("hour_label", F.format_string("%02d:00", F.col("hour")))
        .withColumn("is_am", F.col("hour") < 12)
        .withColumn("day_part", day_part)
        .withColumn("created_at", current_ts)
        .withColumn("updated_at", current_ts)
        .select("time_key", "hour", "hour_label", "is_am", "day_part", "created_at", "updated_at")
    )


def upsert_times(spark, df: DataFrame, mode: str) -> None:
    if mode == "replace" and spark.catalog.tableExists(DIM_TIME_TABLE):
        spark.sql(f"TRUNCATE TABLE {DIM_TIME_TABLE}")

    rows = df.count()
    df.writeTo(DIM_TIME_TABLE).overwritePartitions()
    logging.info("Upserted %d calendar time rows", rows)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    args = parse_args()

    spark = build("gold_dim_calendar_time")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        ensure_table(spark)
        time_df = build_time_dimension(spark)
        upsert_times(spark, time_df, args.mode)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
