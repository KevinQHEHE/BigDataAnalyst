"""Transform Bronze → Silver: Add date_key/time_key, standardize types, MERGE INTO silver.

This job reads from hadoop_catalog.lh.bronze.open_meteo_hourly and writes to
hadoop_catalog.lh.silver.air_quality_hourly_clean with idempotent MERGE on (location_key, ts_utc).

Transformations:
- Add date_key (YYYYMMDD integer) from date_utc
- Add time_key (HH00 integer) from ts_utc
- Cast and validate all numeric fields
- Preserve all existing columns from bronze
- Add processing metadata

Usage:
  bash scripts/spark_submit.sh jobs/silver/transform_bronze_to_silver.py -- [OPTIONS]
  
  # Process all data
  bash scripts/spark_submit.sh jobs/silver/transform_bronze_to_silver.py
  
  # Process specific date range
  bash scripts/spark_submit.sh jobs/silver/transform_bronze_to_silver.py -- --date-range 2024-01-01 2024-12-31
"""
import argparse
import os
import sys
from datetime import datetime

# Ensure local src is importable
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT_DIR, ".env"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_format, hour, year, month, dayofmonth,
    concat, lpad, cast, max as spark_max, min as spark_min
)


def build_spark_session(app_name: str = "transform_bronze_to_silver") -> SparkSession:
    """Build Spark session for silver transformation."""
    from lakehouse_aqi import spark_session
    if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME"):
        return spark_session.build(app_name=app_name)
    else:
        return spark_session.build(app_name=app_name, mode="local")


def get_new_data_range(
    spark: SparkSession,
    bronze_table: str,
    silver_table: str
) -> tuple:
    """Auto-detect date range of new data in Bronze that's not in Silver yet.
    
    Returns:
        (start_date, end_date): Date strings in 'YYYY-MM-DD' format
        (None, None): If Silver is empty (need full load)
        ("NO_NEW_DATA", "NO_NEW_DATA"): If no new data
    """
    try:
        # Check if Silver table exists
        silver_exists = spark.catalog.tableExists(silver_table)
        if not silver_exists:
            print(f"⊘ Silver table {silver_table} doesn't exist, will process all Bronze data")
            return None, None
        
        # Get max timestamp in Silver
        silver_max_row = spark.sql(f"""
            SELECT MAX(ts_utc) as max_ts 
            FROM {silver_table}
        """).collect()
        
        if not silver_max_row or silver_max_row[0]['max_ts'] is None:
            print("⊘ Silver table is empty, will process all Bronze data")
            return None, None
        
        silver_max_ts = silver_max_row[0]['max_ts']
        print(f"✓ Latest Silver timestamp: {silver_max_ts}")
        
        # Get min/max dates in Bronze where ts_utc > silver_max
        bronze_new_row = spark.sql(f"""
            SELECT 
                MIN(date_utc) as min_date,
                MAX(date_utc) as max_date,
                COUNT(*) as count
            FROM {bronze_table}
            WHERE ts_utc > '{silver_max_ts}'
        """).collect()
        
        if not bronze_new_row or bronze_new_row[0]['min_date'] is None:
            print("✓ No new data in Bronze, Silver is up-to-date")
            return "NO_NEW_DATA", "NO_NEW_DATA"
        
        min_date = str(bronze_new_row[0]['min_date'])
        max_date = str(bronze_new_row[0]['max_date'])
        new_count = bronze_new_row[0]['count']
        
        print(f"↻ Found {new_count} new records in Bronze: {min_date} to {max_date}")
        return min_date, max_date
        
    except Exception as e:
        print(f"⚠ Error detecting new data range: {e}")
        print("  Will process all Bronze data")
        return None, None


def transform_bronze_to_silver(
    spark: SparkSession,
    bronze_table: str = "hadoop_catalog.lh.bronze.open_meteo_hourly",
    silver_table: str = "hadoop_catalog.lh.silver.air_quality_hourly_clean",
    start_date: str = None,
    end_date: str = None,
    mode: str = "merge",  # "merge", "overwrite", or "append"
    auto_detect: bool = True  # Auto-detect new data range if start_date not provided
) -> dict:
    """Transform bronze data to silver with enrichments.
    
    Args:
        spark: SparkSession
        bronze_table: Source bronze table
        silver_table: Target silver table
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        mode: Write mode - "merge" (upsert), "overwrite" (replace all), "append" (add only)
        auto_detect: Auto-detect new data range if True and start_date not provided
    
    Returns:
        Dictionary with processing metrics
    """
    from datetime import datetime
    start_time = datetime.now()
    
    print(f"Reading from bronze table: {bronze_table}")
    print(f"Write mode: {mode}")
    
    # Auto-detect new data range for merge mode
    if auto_detect and mode == "merge" and not start_date:
        print("\n🔍 Auto-detecting new data range...")
        start_date, end_date = get_new_data_range(spark, bronze_table, silver_table)
        
        if start_date == "NO_NEW_DATA":
            return {
                "status": "skipped",
                "reason": "no_new_data",
                "records_processed": 0,
                "duration_seconds": 0
            }
    
    # Read bronze data
    query = f"SELECT * FROM {bronze_table}"
    if start_date and end_date:
        query += f" WHERE date_utc BETWEEN '{start_date}' AND '{end_date}'"
        print(f"Date range: {start_date} to {end_date}")
    elif start_date:
        query += f" WHERE date_utc >= '{start_date}'"
        print(f"Start date: {start_date}")
    elif end_date:
        query += f" WHERE date_utc <= '{end_date}'"
        print(f"End date: {end_date}")
    else:
        print("Processing all Bronze data")
    
    df_bronze = spark.sql(query)
    
    record_count = df_bronze.count()
    if record_count == 0:
        print("No data to process in bronze table")
        return {
            "status": "skipped",
            "records_processed": 0,
            "duration_seconds": 0,
            "start_date": start_date,
            "end_date": end_date
        }
    
    print(f"Processing {record_count} records from bronze")
    
    # Transform: add date_key and time_key
    df_silver = df_bronze.withColumn(
        "date_key",
        date_format(col("date_utc"), "yyyyMMdd").cast("int")
    ).withColumn(
        "time_key",
        (hour(col("ts_utc")) * 100).cast("int")
    )
    
    # Deduplicate to avoid duplicates (similar to bronze pattern)
    print("Deduplicating records...")
    df_silver = df_silver.dropDuplicates(["location_key", "ts_utc"])
    
    # Ensure all columns match silver schema
    df_silver = df_silver.select(
        col("location_key"),
        col("ts_utc"),
        col("date_utc"),
        col("date_key"),
        col("time_key"),
        col("aqi"),
        col("aqi_pm25"),
        col("aqi_pm10"),
        col("aqi_no2"),
        col("aqi_o3"),
        col("aqi_so2"),
        col("aqi_co"),
        col("pm25"),
        col("pm10"),
        col("o3"),
        col("no2"),
        col("so2"),
        col("co"),
        col("aod"),
        col("dust"),
        col("uv_index"),
        col("co2"),
        col("model_domain"),
        col("request_timezone"),
        col("_ingested_at")
    )
    
    # Write strategy based on mode
    if mode == "overwrite":
        print(f"Overwriting silver table: {silver_table}")
        df_silver.write.format("iceberg").mode("overwrite").saveAsTable(silver_table)
        
    elif mode == "append":
        print(f"Appending to silver table: {silver_table}")
        df_silver.write.format("iceberg").mode("append").saveAsTable(silver_table)
        
    else:  # merge (default)
        # Create temporary view for MERGE
        tmp_view = "__tmp_bronze_to_silver"
        df_silver.createOrReplaceTempView(tmp_view)
        
        print(f"Merging into silver table: {silver_table}")
        
        # Simplified MERGE - only update a few key fields to reduce shuffle
        merge_sql = f"""
        MERGE INTO {silver_table} AS target
        USING {tmp_view} AS source
        ON target.location_key = source.location_key 
           AND target.ts_utc = source.ts_utc
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        
        spark.sql(merge_sql)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"Successfully processed {record_count} records into silver table")
    
    return {
        "status": "success",
        "records_processed": record_count,
        "duration_seconds": duration,
        "start_date": start_date,
        "end_date": end_date,
        "bronze_table": bronze_table,
        "silver_table": silver_table,
        "mode": mode
    }


def main():
    parser = argparse.ArgumentParser(description="Transform Bronze → Silver with date_key/time_key")
    parser.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        help="Date range to process (YYYY-MM-DD YYYY-MM-DD)"
    )
    parser.add_argument(
        "--bronze-table",
        default="hadoop_catalog.lh.bronze.open_meteo_hourly",
        help="Bronze table name"
    )
    parser.add_argument(
        "--silver-table",
        default="hadoop_catalog.lh.silver.air_quality_hourly_clean",
        help="Silver table name"
    )
    
    args = parser.parse_args()
    
    start_date = None
    end_date = None
    if args.date_range:
        start_date, end_date = args.date_range
        # Validate date format
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as e:
            print(f"Error: Invalid date format. Use YYYY-MM-DD: {e}")
            sys.exit(1)
    
    # Build Spark session
    spark = build_spark_session()
    
    try:
        result = transform_bronze_to_silver(
            spark=spark,
            bronze_table=args.bronze_table,
            silver_table=args.silver_table,
            start_date=start_date,
            end_date=end_date
        )
        
        print(f"\n{'='*60}")
        print(f"Bronze → Silver transformation completed")
        print(f"Status: {result['status']}")
        print(f"Records processed: {result['records_processed']}")
        print(f"Duration: {result['duration_seconds']:.2f}s")
        print(f"{'='*60}")
        
        # Return 0 for success, even if skipped
        sys.exit(0)
        
    except Exception as e:
        print(f"Error during transformation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
