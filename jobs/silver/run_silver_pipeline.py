"""Optimized Silver Transformation - Simple, efficient, Prefect-ready.

Transforms bronze → silver by adding date_key/time_key and MERGE INTO silver table.

Usage:
  python3 jobs/silver/run_silver_pipeline.py --mode [full|incremental]
  
  # Process all data (full refresh)
  bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- --mode full
  
  # Process specific date range
  bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- --mode incremental --start-date 2024-01-01 --end-date 2024-12-31
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


def build_spark_session(app_name: str = "run_silver_pipeline") -> SparkSession:
    from lakehouse_aqi import spark_session
    mode = "cluster" if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME") else "local"
    spark = spark_session.build(app_name=app_name, mode=mode)
    
    # Only runtime-modifiable configs for performance tuning
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    return spark


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
        from transform_bronze_to_silver import transform_bronze_to_silver
        
        spark = build_spark_session()
        spark.conf.set("spark.sql.catalog.hadoop_catalog.warehouse", warehouse)
        
        start_time = time.time()
        
        # Map mode to write strategy
        # full -> overwrite (replace all silver data)
        # incremental -> merge (upsert only changed data)
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
