"""Transform Silver → Gold Fact Hourly: Calculate dominant_pollutant and data_completeness.

This script reads from hadoop_catalog.lh.silver.air_quality_hourly_clean and writes to
hadoop_catalog.lh.gold.fact_air_quality_hourly with:
- dominant_pollutant: argmax(aqi_pm25, aqi_pm10, aqi_o3, aqi_no2, aqi_so2, aqi_co)
- data_completeness: % of measured pollutant columns with non-null values

Transformations:
- Calculate dominant pollutant from AQI sub-indices
- Calculate data completeness (0-100%)
- Add record_id (UUID)
- Preserve all silver columns

Usage:
  bash scripts/spark_submit.sh jobs/gold/transform_fact_hourly.py -- [OPTIONS]
  
  # Process all data (full refresh)
  bash scripts/spark_submit.sh jobs/gold/transform_fact_hourly.py -- --mode full
  
  # Process specific date range (incremental)
  bash scripts/spark_submit.sh jobs/gold/transform_fact_hourly.py -- \
    --mode incremental \
    --start-date 2024-01-01 \
    --end-date 2024-12-31
"""
import argparse
import os
import sys
from datetime import datetime

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
sys.path.insert(0, SRC_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT_DIR, ".env"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, greatest, coalesce, lit, expr,
    sum as _sum, count as _count
)
import uuid as py_uuid


def build_spark_session(app_name: str = "transform_fact_hourly") -> SparkSession:
    from lakehouse_aqi import spark_session
    mode = "cluster" if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME") else "local"
    spark = spark_session.build(app_name=app_name, mode=mode)
    
    # SQL adaptive execution for performance
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    return spark


def get_new_data_range(
    spark: SparkSession,
    silver_table: str,
    gold_table: str
) -> tuple:
    """Auto-detect date range of new data in Silver that's not in Gold yet.
    
    Returns:
        (start_date, end_date): Date strings in 'YYYY-MM-DD' format
        (None, None): If Gold is empty (need full load)
        ("NO_NEW_DATA", "NO_NEW_DATA"): If no new data
    """
    try:
        # Check if Gold table exists
        gold_exists = spark.catalog.tableExists(gold_table)
        if not gold_exists:
            print(f"⊘ Gold table {gold_table} doesn't exist, will process all Silver data")
            return None, None
        
        # Get max timestamp in Gold
        gold_max_row = spark.sql(f"""
            SELECT MAX(ts_utc) as max_ts 
            FROM {gold_table}
        """).collect()
        
        if not gold_max_row or gold_max_row[0]['max_ts'] is None:
            print("⊘ Gold table is empty, will process all Silver data")
            return None, None
        
        gold_max_ts = gold_max_row[0]['max_ts']
        print(f"✓ Latest Gold timestamp: {gold_max_ts}")
        
        # Get min/max dates in Silver where ts_utc > gold_max
        silver_new_row = spark.sql(f"""
            SELECT 
                MIN(date_utc) as min_date,
                MAX(date_utc) as max_date,
                COUNT(*) as count
            FROM {silver_table}
            WHERE ts_utc > '{gold_max_ts}'
        """).collect()
        
        if not silver_new_row or silver_new_row[0]['min_date'] is None:
            print("✓ No new data in Silver, Gold is up-to-date")
            return "NO_NEW_DATA", "NO_NEW_DATA"
        
        min_date = str(silver_new_row[0]['min_date'])
        max_date = str(silver_new_row[0]['max_date'])
        new_count = silver_new_row[0]['count']
        
        print(f"↻ Found {new_count} new records in Silver: {min_date} to {max_date}")
        return min_date, max_date
        
    except Exception as e:
        print(f"⚠ Error detecting new data range: {e}")
        print("  Will process all Silver data")
        return None, None


def transform_fact_hourly(
    spark: SparkSession,
    silver_table: str = "hadoop_catalog.lh.silver.air_quality_hourly_clean",
    gold_table: str = "hadoop_catalog.lh.gold.fact_air_quality_hourly",
    start_date: str = None,
    end_date: str = None,
    mode: str = "overwrite",  # "overwrite" or "merge"
    auto_detect: bool = True  # Auto-detect new data range if start_date not provided
) -> dict:
    """Transform silver to gold fact hourly with enrichments.
    
    Args:
        spark: SparkSession
        silver_table: Source silver table
        gold_table: Target gold fact table
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        mode: Write mode - "overwrite" (replace all) or "merge" (upsert)
        auto_detect: Auto-detect new data range if True and start_date not provided
    
    Returns:
        Dictionary with processing metrics
    """
    start_time = datetime.now()
    
    print(f"Reading from silver table: {silver_table}")
    print(f"Write mode: {mode}")
    
    # Auto-detect new data range for merge mode
    if auto_detect and mode == "merge" and not start_date:
        print("\n🔍 Auto-detecting new data range...")
        start_date, end_date = get_new_data_range(spark, silver_table, gold_table)
        
        if start_date == "NO_NEW_DATA":
            return {
                "status": "skipped",
                "reason": "no_new_data",
                "records_processed": 0,
                "duration_seconds": 0
            }
    
    # Read silver data
    query = f"SELECT * FROM {silver_table}"
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
        print("Processing all Silver data")
    
    df_silver = spark.sql(query)
    
    record_count = df_silver.count()
    if record_count == 0:
        print("No data to process in silver table")
        return {
            "status": "skipped",
            "records_processed": 0,
            "duration_seconds": 0
        }
    
    print(f"Processing {record_count} records from silver")
    
    # Calculate dominant pollutant
    # Find max AQI sub-index and corresponding pollutant name
    df_gold = df_silver.withColumn(
        "max_aqi_value",
        greatest(
            coalesce(col("aqi_pm25"), lit(0)),
            coalesce(col("aqi_pm10"), lit(0)),
            coalesce(col("aqi_o3"), lit(0)),
            coalesce(col("aqi_no2"), lit(0)),
            coalesce(col("aqi_so2"), lit(0)),
            coalesce(col("aqi_co"), lit(0))
        )
    ).withColumn(
        "dominant_pollutant",
        when(col("max_aqi_value") == 0, lit(None))
        .when(col("max_aqi_value") == coalesce(col("aqi_pm25"), lit(0)), lit("pm25"))
        .when(col("max_aqi_value") == coalesce(col("aqi_pm10"), lit(0)), lit("pm10"))
        .when(col("max_aqi_value") == coalesce(col("aqi_o3"), lit(0)), lit("o3"))
        .when(col("max_aqi_value") == coalesce(col("aqi_no2"), lit(0)), lit("no2"))
        .when(col("max_aqi_value") == coalesce(col("aqi_so2"), lit(0)), lit("so2"))
        .when(col("max_aqi_value") == coalesce(col("aqi_co"), lit(0)), lit("co"))
        .otherwise(lit(None))
    ).drop("max_aqi_value")
    
    # Calculate data completeness (% of pollutant columns with values)
    # Pollutant columns: pm25, pm10, o3, no2, so2, co, aod, dust, uv_index, co2 (10 total)
    pollutant_cols = ["pm25", "pm10", "o3", "no2", "so2", "co", "aod", "dust", "uv_index", "co2"]
    
    # Count non-null pollutant values
    non_null_count = sum([when(col(c).isNotNull(), 1).otherwise(0) for c in pollutant_cols])
    
    df_gold = df_gold.withColumn(
        "data_completeness",
        (non_null_count / lit(len(pollutant_cols)) * 100.0)
    )
    
    # Add record_id (UUID)
    df_gold = df_gold.withColumn("record_id", expr("uuid()"))
    
    # Select columns in schema order
    df_gold = df_gold.select(
        col("record_id"),
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
        col("dominant_pollutant"),
        col("data_completeness")
    )
    
    # Deduplicate on natural key
    print("Deduplicating records on (location_key, ts_utc)...")
    df_gold = df_gold.dropDuplicates(["location_key", "ts_utc"])
    
    # Write to gold
    if mode == "overwrite":
        print(f"Overwriting gold fact table: {gold_table}")
        df_gold.write.format("iceberg").mode("overwrite").saveAsTable(gold_table)
    else:
        print(f"Merging into gold fact table: {gold_table}")
        tmp_view = "__tmp_fact_hourly"
        df_gold.createOrReplaceTempView(tmp_view)
        
        merge_sql = f"""
        MERGE INTO {gold_table} AS target
        USING {tmp_view} AS source
        ON target.location_key = source.location_key 
           AND target.ts_utc = source.ts_utc
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(merge_sql)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"Successfully processed {record_count} records into gold fact hourly")
    
    return {
        "status": "success",
        "records_processed": record_count,
        "duration_seconds": duration,
        "mode": mode
    }


def main():
    parser = argparse.ArgumentParser(description="Transform Silver → Gold Fact Hourly")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--silver-table", default="hadoop_catalog.lh.silver.air_quality_hourly_clean")
    parser.add_argument("--gold-table", default="hadoop_catalog.lh.gold.fact_air_quality_hourly")
    
    args = parser.parse_args()
    
    # Map mode to write strategy
    write_mode = "overwrite" if args.mode == "full" else "merge"
    
    spark = build_spark_session()
    
    try:
        result = transform_fact_hourly(
            spark=spark,
            silver_table=args.silver_table,
            gold_table=args.gold_table,
            start_date=args.start_date,
            end_date=args.end_date,
            mode=write_mode
        )
        
        print(f"\n{'='*60}")
        print(f"Gold Fact Hourly transformation completed")
        print(f"Status: {result['status']}")
        print(f"Records processed: {result['records_processed']}")
        print(f"Duration: {result['duration_seconds']:.2f}s")
        print(f"{'='*60}")
        
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
