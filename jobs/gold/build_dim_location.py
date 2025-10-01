import argparse
import json
import logging
from typing import Iterable, List

from pyspark.sql import DataFrame, functions as F, types as T

from aq_lakehouse.spark_session import build

DIM_LOCATION_TABLE = "hadoop_catalog.aq.gold.dim_location"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--locations-config",
        default="configs/locations.json",
        help="Path to JSON file containing location metadata",
    )
    parser.add_argument(
        "--location-id",
        dest="location_ids",
        action="append",
        default=[],
        help="Optional list of location IDs to process",
    )
    return parser.parse_args()


def load_location_records(path: str, location_ids: Iterable[str]) -> List[dict]:
    with open(path, "r", encoding="utf-8") as handle:
        raw = json.load(handle)

    allow = set(location_ids) if location_ids else None
    records: List[dict] = []
    for key, value in raw.items():
        if allow and key not in allow:
            continue

        records.append(
            {
                "location_key": key,
                "location_name": value.get("name", key),
                "latitude": value.get("latitude"),
                "longitude": value.get("longitude"),
                "country": value.get("country"),
                "admin1": value.get("admin1"),
                "timezone": value.get("timezone"),
                "is_active": value.get("is_active", True),
                "valid_from": value.get("valid_from"),
                "valid_to": value.get("valid_to"),
            }
        )
    return records


def ensure_table(spark) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.aq.gold")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {DIM_LOCATION_TABLE} (
          location_key STRING,
          location_name STRING,
          latitude DOUBLE,
          longitude DOUBLE,
          country STRING,
          admin1 STRING,
          timezone STRING,
          is_active BOOLEAN,
          valid_from DATE,
          valid_to DATE,
          created_at TIMESTAMP,
          updated_at TIMESTAMP
        ) USING iceberg
        """
    )
    spark.sql(
        f"""
        ALTER TABLE {DIM_LOCATION_TABLE} SET TBLPROPERTIES (
          'format-version'='2',
          'write.target-file-size-bytes'='33554432',
          'write.distribution-mode'='hash'
        )
        """
    )


def build_dataframe(spark, config_path: str, location_ids: Iterable[str]) -> DataFrame | None:
    records = load_location_records(config_path, location_ids)
    if not records:
        logging.info("No locations found in %s; skipping dim_location", config_path)
        return None

    schema = T.StructType(
        [
            T.StructField("location_key", T.StringType()),
            T.StructField("location_name", T.StringType()),
            T.StructField("latitude", T.DoubleType()),
            T.StructField("longitude", T.DoubleType()),
            T.StructField("country", T.StringType()),
            T.StructField("admin1", T.StringType()),
            T.StructField("timezone", T.StringType()),
            T.StructField("is_active", T.BooleanType()),
            T.StructField("valid_from", T.StringType()),
            T.StructField("valid_to", T.StringType()),
        ]
    )

    df = spark.createDataFrame(records, schema)
    current_ts = F.current_timestamp()

    result = (
        df
        .withColumn("location_name", F.coalesce(F.col("location_name"), F.col("location_key")))
        .withColumn("latitude", F.col("latitude").cast("double"))
        .withColumn("longitude", F.col("longitude").cast("double"))
        .withColumn("is_active", F.col("is_active").cast("boolean"))
        .withColumn("valid_from", F.col("valid_from").cast("date"))
        .withColumn("valid_to", F.col("valid_to").cast("date"))
        .select(
            "location_key",
            "location_name",
            "latitude",
            "longitude",
            "country",
            "admin1",
            "timezone",
            "is_active",
            "valid_from",
            "valid_to",
        )
        .withColumn("created_at", current_ts)
        .withColumn("updated_at", current_ts)
    )
    return result


def filter_new_or_changed(spark, df: DataFrame) -> DataFrame:
    if not spark.catalog.tableExists(DIM_LOCATION_TABLE):
        return df

    existing = (
        spark.table(DIM_LOCATION_TABLE)
        .select(
            "location_key",
            "location_name",
            "latitude",
            "longitude",
            "country",
            "admin1",
            "timezone",
            "is_active",
            "valid_from",
            "valid_to",
        )
        .withColumn(
            "payload",
            F.struct(
                "location_name",
                "latitude",
                "longitude",
                "country",
                "admin1",
                "timezone",
                "is_active",
                "valid_from",
                "valid_to",
            ),
        )
    )

    candidates = df.withColumn(
        "payload",
        F.struct(
            "location_name",
            "latitude",
            "longitude",
            "country",
            "admin1",
            "timezone",
            "is_active",
            "valid_from",
            "valid_to",
        ),
    )

    filtered = (
        candidates.alias("s")
        .join(existing.alias("t"), "location_key", "left")
        .where(F.col("t.payload").isNull() | (F.col("s.payload") != F.col("t.payload")))
        .select("s.*")
    )

    return filtered.drop("payload")


def merge_dimension(spark, df: DataFrame) -> None:
    temp_view = "tmp_dim_location_merge"
    df.createOrReplaceTempView(temp_view)
    try:
        spark.sql(
            f"""
            MERGE INTO {DIM_LOCATION_TABLE} t
            USING {temp_view} s
            ON t.location_key = s.location_key
            WHEN MATCHED THEN UPDATE SET
              location_name = COALESCE(s.location_name, t.location_name),
              latitude = COALESCE(s.latitude, t.latitude),
              longitude = COALESCE(s.longitude, t.longitude),
              country = COALESCE(s.country, t.country),
              admin1 = COALESCE(s.admin1, t.admin1),
              timezone = COALESCE(s.timezone, t.timezone),
              is_active = COALESCE(s.is_active, t.is_active),
              valid_from = COALESCE(s.valid_from, t.valid_from),
              valid_to = COALESCE(s.valid_to, t.valid_to),
              updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT (
              location_key,
              location_name,
              latitude,
              longitude,
              country,
              admin1,
              timezone,
              is_active,
              valid_from,
              valid_to,
              created_at,
              updated_at
            ) VALUES (
              s.location_key,
              s.location_name,
              s.latitude,
              s.longitude,
              s.country,
              s.admin1,
              s.timezone,
              s.is_active,
              s.valid_from,
              s.valid_to,
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

    spark = build("gold_dim_location")
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    try:
        ensure_table(spark)
        df = build_dataframe(spark, args.locations_config, args.location_ids)
        if df is None:
            return

        df = filter_new_or_changed(spark, df)

        rows = df.count()
        if rows == 0:
            logging.info("No new or changed locations detected; skipping dim_location merge")
            return

        logging.info("Preparing to upsert %d dim_location rows", rows)
        merge_dimension(spark, df)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
