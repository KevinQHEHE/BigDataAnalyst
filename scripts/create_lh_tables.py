"""Create Iceberg namespaces and tables for LH (bronze/silver/gold).

Usage:
  python scripts/create_lh_tables.py [--dry-run] [--warehouse WAREHOUSE_URI]

This script connects to a Spark session (local for dev if not submitted) and runs
CREATE NAMESPACE / CREATE TABLE statements for the required dims and facts.

If --dry-run is provided, SQL statements will be printed instead of executed.
"""
import argparse
import json
import os
import sys
from typing import List

# Ensure local src is importable when running scripts directly
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

# Load .env file if present
from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT_DIR, ".env"))

from pyspark.sql import SparkSession


def build_spark_session(app_name: str = "create_lh_tables") -> SparkSession:
    """Build Spark session, auto-detect if running under spark-submit or local dev."""
    from lakehouse_aqi import spark_session
    # If SPARK_MASTER env is set or we're in spark-submit, don't force local mode
    if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME"):
        return spark_session.build(app_name=app_name)
    else:
        return spark_session.build(app_name=app_name, mode="local")


def load_locations(cfg_path: str) -> List[dict]:
    if not os.path.exists(cfg_path):
        return []
    with open(cfg_path, "r", encoding="utf-8") as f:
        return json.load(f)


def run_or_print(spark: SparkSession, sql: str, dry_run: bool):
    print("-- SQL: ", sql)
    if not dry_run:
        spark.sql(sql)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--warehouse", default=os.getenv("WAREHOUSE_URI", "hdfs://khoa-master:9000/warehouse/iceberg"))
    p.add_argument("--locations", default="configs/locations.json")
    args = p.parse_args()

    spark = build_spark_session()
    spark.conf.set("spark.sql.catalog.hadoop_catalog.warehouse", args.warehouse)

    dry = args.dry_run

    # Create namespaces (use hadoop_catalog prefix for multi-part namespaces)
    namespaces = ["hadoop_catalog.lh.bronze", "hadoop_catalog.lh.silver", "hadoop_catalog.lh.gold"]
    for ns in namespaces:
        run_or_print(spark, f"CREATE NAMESPACE IF NOT EXISTS {ns}", dry)

    # Dimensions
    # dim_date: date_key, date_utc, year, month, day, dow
    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.dim_date (
          date_key STRING,
          date_utc DATE,
          year INT,
          month INT,
          day INT,
          dow INT
        ) USING iceberg PARTITIONED BY (days(date_utc))
        """,
        dry,
    )

    # dim_time: hour 0-23
    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.dim_time (
          hour INT
        ) USING iceberg
        """,
        dry,
    )

    # dim_location: location_key, location_name, latitude, longitude
    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.dim_location (
          location_key STRING,
          location_name STRING,
          latitude DOUBLE,
          longitude DOUBLE
        ) USING iceberg
        """,
        dry,
    )

    # dim_pollutant: code, name
    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.dim_pollutant (
          code STRING,
          name STRING
        ) USING iceberg
        """,
        dry,
    )

    # Bronze table (raw)
    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.bronze.open_meteo_hourly (
          location_id STRING,
          latitude DOUBLE,
          longitude DOUBLE,
          ts TIMESTAMP,
          aerosol_optical_depth DOUBLE,
          pm2_5 DOUBLE,
          pm10 DOUBLE,
          dust DOUBLE,
          nitrogen_dioxide DOUBLE,
          ozone DOUBLE,
          sulphur_dioxide DOUBLE,
          carbon_monoxide DOUBLE,
          uv_index DOUBLE,
          uv_index_clear_sky DOUBLE,
          source STRING,
          run_id STRING,
          ingested_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (days(ts))
        """,
        dry,
    )

    # Silver table (cleaned)
    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.silver.air_quality_hourly_clean (
          location_key STRING,
          ts TIMESTAMP,
          pm2_5 DOUBLE,
          pm10 DOUBLE,
          ozone DOUBLE,
          nitrogen_dioxide DOUBLE,
          sulphur_dioxide DOUBLE,
          carbon_monoxide DOUBLE,
          run_id STRING,
          ingested_at TIMESTAMP
        ) USING iceberg PARTITIONED BY (days(ts))
        """,
        dry,
    )

    # Gold facts
    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.fact_air_quality_hourly (
          location_key STRING,
          date_utc DATE,
          hour INT,
          pm2_5 DOUBLE,
          pm10 DOUBLE,
          ozone DOUBLE
        ) USING iceberg PARTITIONED BY (days(date_utc), location_key)
        """,
        dry,
    )

    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.fact_city_daily (
          location_key STRING,
          date_utc DATE,
          avg_pm2_5 DOUBLE,
          max_pm10 DOUBLE
        ) USING iceberg PARTITIONED BY (days(date_utc), location_key)
        """,
        dry,
    )

    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.fact_episode (
          episode_id STRING,
          location_key STRING,
          start_date_utc DATE,
          end_date_utc DATE,
          severity STRING
        ) USING iceberg PARTITIONED BY (days(start_date_utc), location_key)
        """,
        dry,
    )

    # Seed dims
    locations = load_locations(args.locations)
    # Seed dim_time (0-23)
    time_rows = ",".join([f"({h})" for h in range(24)])
    run_or_print(spark, f"INSERT INTO hadoop_catalog.lh.gold.dim_time VALUES {time_rows}", dry)

    # Seed dim_location (take at least two from configs)
    if locations:
        loc_vals = []
        for loc in locations[:5]:
            key = loc.get("location_key") or loc.get("id") or loc.get("name")
            name = loc.get("location_name") or loc.get("name")
            lat = loc.get("latitude")
            lon = loc.get("longitude")
            # sanitize single quotes in names
            if name is None:
                name = ""
            name = str(name).replace("'", "\\'")
            loc_vals.append(f"('{key}','{name}',{lat},{lon})")
        if loc_vals:
            run_or_print(spark, f"INSERT INTO hadoop_catalog.lh.gold.dim_location VALUES {','.join(loc_vals)}", dry)

    # Seed dim_pollutant minimal set
    pollutants = [
        ("pm2_5", "Particulate matter <2.5µm"),
        ("pm10", "Particulate matter <10µm"),
        ("ozone", "Ozone"),
    ]
    poll_vals = ",".join([f"('{c}','{n}')" for c, n in pollutants])
    run_or_print(spark, f"INSERT INTO hadoop_catalog.lh.gold.dim_pollutant VALUES {poll_vals}", dry)

    print("Done (dry_run=" + str(dry) + ")")


if __name__ == "__main__":
    main()
