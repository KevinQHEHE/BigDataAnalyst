"""Optimized Silver Transformation - Simple, efficient, Prefect-ready.

Transforms bronze → silver by adding date_key/time_key and MERGE INTO silver table.

Usage:
  python3 jobs/silver/run_silver_pipeline.py --mode [full|incremental]
  
  # Process all data (full refresh)
  bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- --mode full
  
  # Process specific date range
  bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- --mode incremental --start-date 2024-01-01 --end-date 2024-12-31
  
  # Incremental with auto-detect (default)
  bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- --mode incremental
"""
import argparse
import os
import sys
import time
from datetime import datetime
from typing import Dict, Optional

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
sys.path.insert(0, SRC_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT_DIR, ".env"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, hour


def build_spark_session(app_name: str = "run_silver_pipeline") -> SparkSession:
    from lakehouse_aqi import spark_session
    mode = "cluster" if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME") else "local"
    spark = spark_session.build(app_name=app_name, mode=mode)
    
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    return spark


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
        silver_exists = spark.catalog.tableExists(silver_table)
        if not silver_exists:
            print(f"Silver table {silver_table} doesn't exist, will process all Bronze data")
            return None, None
        
        silver_max_row = spark.sql(f"SELECT MAX(ts_utc) as max_ts FROM {silver_table}").collect()
        
        if not silver_max_row or silver_max_row[0]['max_ts'] is None:
            print("Silver table is empty, will process all Bronze data")
            return None, None
        
        silver_max_ts = silver_max_row[0]['max_ts']
        print(f"Latest Silver timestamp: {silver_max_ts}")
        
        bronze_new_row = spark.sql(f"""
            SELECT 
                MIN(date_utc) as min_date,
                MAX(date_utc) as max_date,
                COUNT(*) as count
            FROM {bronze_table}
            WHERE ts_utc > '{silver_max_ts}'
        """).collect()
        
        if not bronze_new_row or bronze_new_row[0]['min_date'] is None:
            print("No new data in Bronze, Silver is up-to-date")
            return "NO_NEW_DATA", "NO_NEW_DATA"
        
        min_date = str(bronze_new_row[0]['min_date'])
        max_date = str(bronze_new_row[0]['max_date'])
        new_count = bronze_new_row[0]['count']
        
        print(f"Found {new_count} new records in Bronze: {min_date} to {max_date}")
        return min_date, max_date
        
    except Exception as e:
        print(f"Error detecting new data range: {e}")
        print("Will process all Bronze data")
        return None, None


def transform_bronze_to_silver(
    spark: SparkSession,
    bronze_table: str,
    silver_table: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    mode: str = "merge",
    auto_detect: bool = True
) -> Dict:
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
    print(f"Reading from bronze table: {bronze_table}")
    print(f"Write mode: {mode}")
    
    if auto_detect and mode == "merge" and not start_date:
        print("\nAuto-detecting new data range...")
        start_date, end_date = get_new_data_range(spark, bronze_table, silver_table)
        
        if start_date == "NO_NEW_DATA":
            return {
                "status": "skipped",
                "reason": "no_new_data",
                "records_processed": 0
            }
    
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
            "start_date": start_date,
            "end_date": end_date
        }
    
    print(f"Processing {record_count} records from bronze")
    
    df_silver = df_bronze.withColumn(
        "date_key",
        date_format(col("date_utc"), "yyyyMMdd").cast("int")
    ).withColumn(
        "time_key",
        (hour(col("ts_utc")) * 100).cast("int")
    )
    
    print("Deduplicating records...")
    df_silver = df_silver.dropDuplicates(["location_key", "ts_utc"])
    
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
    
    if mode == "overwrite":
        print(f"Overwriting silver table: {silver_table}")
        df_silver.write.format("iceberg").mode("overwrite").saveAsTable(silver_table)
        
    elif mode == "append":
        print(f"Appending to silver table: {silver_table}")
        df_silver.write.format("iceberg").mode("append").saveAsTable(silver_table)
        
    else:
        tmp_view = "__tmp_bronze_to_silver"
        df_silver.createOrReplaceTempView(tmp_view)
        
        print(f"Merging into silver table: {silver_table}")
        
        merge_sql = f"""
        MERGE INTO {silver_table} AS target
        USING {tmp_view} AS source
        ON target.location_key = source.location_key 
           AND target.ts_utc = source.ts_utc
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        
        spark.sql(merge_sql)
    
    print(f"Successfully processed {record_count} records into silver table")
    
    return {
        "status": "success",
        "records_processed": record_count,
        "start_date": start_date,
        "end_date": end_date,
        "bronze_table": bronze_table,
        "silver_table": silver_table,
        "mode": mode
    }


def execute_silver_transformation(
    mode: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    bronze_table: str = "hadoop_catalog.lh.bronze.open_meteo_hourly",
    silver_table: str = "hadoop_catalog.lh.silver.air_quality_hourly_clean",
    warehouse: str = "hdfs://khoa-master:9000/warehouse/iceberg"
) -> Dict:
    """Prefect-friendly silver transformation function."""
    try:
        spark = build_spark_session()
        spark.conf.set("spark.sql.catalog.hadoop_catalog.warehouse", warehouse)
        
        start_time = time.time()
        
        write_mode = "overwrite" if mode == "full" else "merge"
        
        print(f"SILVER TRANSFORMATION: mode={mode}, write_mode={write_mode}")
        if start_date or end_date:
            print(f"Date range: {start_date or 'earliest'} to {end_date or 'latest'}")
        
        result = transform_bronze_to_silver(
            spark=spark,
            bronze_table=bronze_table,
            silver_table=silver_table,
            start_date=start_date,
            end_date=end_date,
            mode=write_mode
        )
        
        elapsed = time.time() - start_time
        
        print(f"\nCOMPLETE: {result.get('records_processed', 0)} rows in {elapsed:.1f}s")
        
        spark.stop()
        
        return {
            "success": True,
            "stats": result,
            "elapsed_seconds": elapsed
        }
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Optimized Silver Transformation")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--bronze-table", default="hadoop_catalog.lh.bronze.open_meteo_hourly")
    parser.add_argument("--silver-table", default="hadoop_catalog.lh.silver.air_quality_hourly_clean")
    parser.add_argument("--warehouse", default=os.getenv("WAREHOUSE_URI", "hdfs://khoa-master:9000/warehouse/iceberg"))
    
    args = parser.parse_args()
    
    result = execute_silver_transformation(
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        bronze_table=args.bronze_table,
        silver_table=args.silver_table,
        warehouse=args.warehouse
    )
    
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
