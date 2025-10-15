"""Create Iceberg namespaces and tables for LH (bronze/silver/gold).

Usage:
  python scripts/create_lh_tables.py [--dry-run] [--warehouse WAREHOUSE_URI] [--drop-all]

This script connects to a Spark session (local for dev if not submitted) and runs
CREATE NAMESPACE / CREATE TABLE statements for the required dims and facts.

If --dry-run is provided, SQL statements will be printed instead of executed.
If --drop-all is provided, all existing tables will be dropped before creating new ones.
"""
import argparse
import os
import sys

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

def run_or_print(spark: SparkSession, sql: str, dry_run: bool):
    print("-- SQL: ", sql)
    if not dry_run:
        try:
            spark.sql(sql)
            print("   ✓ Success")
        except Exception as e:
            print(f"   ✗ Error: {e}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--warehouse", default=os.getenv("WAREHOUSE_URI", "hdfs://khoa-master:9000/warehouse/iceberg"))
    p.add_argument("--drop-all", action="store_true", help="Drop all existing tables before creating new ones")
    args = p.parse_args()

    # Respect dry-run: only build a Spark session when actually executing
    dry = args.dry_run
    spark = None
    if not dry:
        spark = build_spark_session()
        spark.conf.set("spark.sql.catalog.hadoop_catalog.warehouse", args.warehouse)

    # Drop all tables if requested
    if args.drop_all:
        print("=" * 60)
        print("DROPPING ALL EXISTING TABLES")
        print("=" * 60)
        
        # Drop tables in reverse dependency order
        print("\n### Dropping GOLD Fact Tables ###")
        run_or_print(spark, "DROP TABLE IF EXISTS hadoop_catalog.lh.gold.fact_episode", dry)
        run_or_print(spark, "DROP TABLE IF EXISTS hadoop_catalog.lh.gold.fact_city_daily", dry)
        run_or_print(spark, "DROP TABLE IF EXISTS hadoop_catalog.lh.gold.fact_air_quality_hourly", dry)

        print("\n### Dropping GOLD Dimension Tables ###")
        run_or_print(spark, "DROP TABLE IF EXISTS hadoop_catalog.lh.gold.dim_pollutant", dry)
        run_or_print(spark, "DROP TABLE IF EXISTS hadoop_catalog.lh.gold.dim_location", dry)
        run_or_print(spark, "DROP TABLE IF EXISTS hadoop_catalog.lh.gold.dim_time", dry)
        run_or_print(spark, "DROP TABLE IF EXISTS hadoop_catalog.lh.gold.dim_date", dry)

        print("\n### Dropping SILVER Tables ###")
        run_or_print(spark, "DROP TABLE IF EXISTS hadoop_catalog.lh.silver.air_quality_hourly_clean", dry)

        print("\n### Dropping BRONZE Tables ###")
        run_or_print(spark, "DROP TABLE IF EXISTS hadoop_catalog.lh.bronze.open_meteo_hourly", dry)
        
        print("\n" + "=" * 60)
        print("DROP COMPLETE - Now creating tables...")
        print("=" * 60 + "\n")

    # Create namespaces (use hadoop_catalog prefix for multi-part namespaces)
    namespaces = ["hadoop_catalog.lh.bronze", "hadoop_catalog.lh.silver", "hadoop_catalog.lh.gold"]
    for ns in namespaces:
        run_or_print(spark, f"CREATE NAMESPACE IF NOT EXISTS {ns}", dry)

    # Dimensions
    # dim_date: date_key (YYYYMMDD), date_value, day_of_month, day_of_week, week_of_year, month, month_name, quarter, year, is_weekend
    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.dim_date (
          date_key INT,
          date_value DATE,
          day_of_month INT,
          day_of_week INT,
          week_of_year INT,
          month INT,
          month_name STRING,
          quarter INT,
          year INT,
          is_weekend BOOLEAN
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2')
        """,
        dry,
    )

    # dim_time: time_key (HH00), time_value, hour, work_shift
    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.dim_time (
          time_key INT,
          time_value STRING,
          hour INT,
          work_shift STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2')
        """,
        dry,
    )

    # dim_location: location_key, location_name, latitude, longitude, timezone
    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.dim_location (
          location_key STRING,
          location_name STRING,
          latitude DOUBLE,
          longitude DOUBLE,
          timezone STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2')
        """,
        dry,
    )

    # dim_pollutant: pollutant_code, display_name, unit_default, aqi_timespan
    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.dim_pollutant (
          pollutant_code STRING,
          display_name STRING,
          unit_default STRING,
          aqi_timespan STRING
        ) USING iceberg
        TBLPROPERTIES ('format-version'='2')
        """,
        dry,
    )

    # Bronze table (raw landing from Open-Meteo)
    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.bronze.open_meteo_hourly (
          location_key STRING NOT NULL,
          ts_utc TIMESTAMP NOT NULL,
          date_utc DATE NOT NULL,
          latitude DOUBLE,
          longitude DOUBLE,
          aqi INT,
          aqi_pm25 INT,
          aqi_pm10 INT,
          aqi_no2 INT,
          aqi_o3 INT,
          aqi_so2 INT,
          aqi_co INT,
          pm25 DOUBLE,
          pm10 DOUBLE,
          o3 DOUBLE,
          no2 DOUBLE,
          so2 DOUBLE,
          co DOUBLE,
          aod DOUBLE,
          dust DOUBLE,
          uv_index DOUBLE,
          co2 DOUBLE,
          model_domain STRING,
          request_timezone STRING,
          _ingested_at TIMESTAMP
        ) USING PARQUET
        PARTITIONED BY (date_utc, location_key)
        TBLPROPERTIES ('parquet.compression'='ZSTD')
        """,
        dry,
    )

    # Silver table (standardized & enriched)
    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.silver.air_quality_hourly_clean (
          location_key STRING NOT NULL,
          ts_utc TIMESTAMP NOT NULL,
          date_utc DATE NOT NULL,
          date_key INT,
          time_key INT,
          aqi INT,
          aqi_pm25 INT,
          aqi_pm10 INT,
          aqi_no2 INT,
          aqi_o3 INT,
          aqi_so2 INT,
          aqi_co INT,
          pm25 DOUBLE,
          pm10 DOUBLE,
          o3 DOUBLE,
          no2 DOUBLE,
          so2 DOUBLE,
          co DOUBLE,
          aod DOUBLE,
          dust DOUBLE,
          uv_index DOUBLE,
          co2 DOUBLE,
          model_domain STRING,
          request_timezone STRING,
          _ingested_at TIMESTAMP
        ) USING PARQUET
        PARTITIONED BY (date_utc, location_key)
        TBLPROPERTIES ('parquet.compression'='ZSTD')
        """,
        dry,
    )

    # Gold facts
    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.fact_air_quality_hourly (
          record_id STRING,
          location_key STRING NOT NULL,
          ts_utc TIMESTAMP NOT NULL,
          date_utc DATE NOT NULL,
          date_key INT,
          time_key INT,
          aqi INT,
          aqi_pm25 INT,
          aqi_pm10 INT,
          aqi_no2 INT,
          aqi_o3 INT,
          aqi_so2 INT,
          aqi_co INT,
          pm25 DOUBLE,
          pm10 DOUBLE,
          o3 DOUBLE,
          no2 DOUBLE,
          so2 DOUBLE,
          co DOUBLE,
          aod DOUBLE,
          dust DOUBLE,
          uv_index DOUBLE,
          co2 DOUBLE,
          dominant_pollutant STRING,
          data_completeness DOUBLE
        ) USING PARQUET
        PARTITIONED BY (date_utc, location_key)
        TBLPROPERTIES ('parquet.compression'='ZSTD')
        """,
        dry,
    )

    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.fact_city_daily (
          daily_record_id STRING,
          location_key STRING NOT NULL,
          date_utc DATE NOT NULL,
          date_key INT,
          aqi_daily_max INT,
          dominant_pollutant_daily STRING,
          hours_in_cat_good INT,
          hours_in_cat_moderate INT,
          hours_in_cat_usg INT,
          hours_in_cat_unhealthy INT,
          hours_in_cat_very_unhealthy INT,
          hours_in_cat_hazardous INT,
          hours_measured INT,
          data_completeness DOUBLE
        ) USING PARQUET
        PARTITIONED BY (date_utc, location_key)
        TBLPROPERTIES ('parquet.compression'='ZSTD')
        """,
        dry,
    )

    run_or_print(
        spark,
        """
        CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.fact_episode (
          episode_id STRING,
          location_key STRING NOT NULL,
          start_ts_utc TIMESTAMP NOT NULL,
          end_ts_utc TIMESTAMP NOT NULL,
          start_date_utc DATE NOT NULL,
          duration_hours INT,
          peak_aqi INT,
          hours_flagged INT,
          dominant_pollutant STRING,
          rule_code STRING
        ) USING PARQUET
        PARTITIONED BY (start_date_utc, location_key)
        TBLPROPERTIES ('parquet.compression'='ZSTD')
        """,
        dry,
    )

    print("DONE (dry_run=" + str(dry) + ")")


if __name__ == "__main__":
    main()
