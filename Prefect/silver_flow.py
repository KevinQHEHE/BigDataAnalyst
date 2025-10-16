"""Prefect Flow for Silver Layer Transformation.

This flow orchestrates the Silver layer transformation (Bronze → Silver) with:
- Single SparkSession per flow execution (YARN-compatible)
- Automatic retry on failure
- Support for full refresh and incremental modes
- Data quality validation
- Proper logging and metrics

Usage (via YARN):
  bash scripts/spark_submit.sh Prefect/silver_flow.py -- --mode incremental
  bash scripts/spark_submit.sh Prefect/silver_flow.py -- --mode full --start-date 2024-01-01 --end-date 2024-12-31

DO NOT run directly with python - use spark_submit.sh wrapper for YARN deployment.
"""
import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional

from prefect import flow, task

# Setup paths
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
JOBS_DIR = ROOT_DIR / "jobs"

for path in [SRC_DIR, JOBS_DIR]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from dotenv import load_dotenv
load_dotenv(ROOT_DIR / ".env")

# Import Spark context manager
from spark_context import get_spark_session, log_spark_info, validate_yarn_mode


@task(
    name="Transform Bronze to Silver",
    description="Execute bronze to silver transformation with date_key/time_key enrichment",
    retries=2,
    retry_delay_seconds=60,
    log_prints=True
)
def transform_bronze_to_silver_task(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    write_mode: str = "merge",
    bronze_table: str = "hadoop_catalog.lh.bronze.open_meteo_hourly",
    silver_table: str = "hadoop_catalog.lh.silver.air_quality_hourly_clean"
) -> Dict:
    """Execute bronze to silver transformation.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        write_mode: "merge" (upsert), "overwrite" (replace), or "append"
        bronze_table: Source bronze table
        silver_table: Target silver table
        
    Returns:
        Dictionary with transformation metrics
    """
    from silver.transform_bronze_to_silver import transform_bronze_to_silver
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    print(f"Bronze → Silver Transformation")
    print(f"  Mode: {write_mode}")
    print(f"  Bronze: {bronze_table}")
    print(f"  Silver: {silver_table}")
    
    if start_date and end_date:
        print(f"  Date range: {start_date} to {end_date}")
    elif start_date:
        print(f"  From: {start_date}")
    elif end_date:
        print(f"  Until: {end_date}")
    else:
        print(f"  Processing: All data")
    
    result = transform_bronze_to_silver(
        spark=spark,
        bronze_table=bronze_table,
        silver_table=silver_table,
        start_date=start_date,
        end_date=end_date,
        mode=write_mode,
        auto_detect=True  # Enable auto-detection for incremental processing
    )
    
    print(f"✓ Transformation complete")
    print(f"  Records processed: {result.get('records_processed', 0)}")
    print(f"  Duration: {result.get('duration_seconds', 0):.1f}s")
    
    return result


@task(
    name="Validate Silver Data",
    description="Run data quality checks on silver table",
    retries=1,
    retry_delay_seconds=30,
    log_prints=True
)
def validate_silver_task(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    silver_table: str = "hadoop_catalog.lh.silver.air_quality_hourly_clean"
) -> Dict:
    """Validate silver data quality.
    
    Args:
        start_date: Start date for validation scope
        end_date: End date for validation scope
        silver_table: Silver table to validate
        
    Returns:
        Dictionary with validation results
    """
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    print(f"Validating silver table: {silver_table}")
    
    # Build WHERE clause
    where_clause = ""
    if start_date and end_date:
        where_clause = f"WHERE date_utc BETWEEN '{start_date}' AND '{end_date}'"
    elif start_date:
        where_clause = f"WHERE date_utc >= '{start_date}'"
    elif end_date:
        where_clause = f"WHERE date_utc <= '{end_date}'"
    
    # Check total records
    total_query = f"SELECT COUNT(*) as cnt FROM {silver_table} {where_clause}"
    total_records = spark.sql(total_query).collect()[0]['cnt']
    
    # Check for duplicates on primary key
    dup_query = f"""
    SELECT COUNT(*) as dup_cnt FROM (
        SELECT location_key, ts_utc, COUNT(*) as cnt
        FROM {silver_table}
        {where_clause}
        GROUP BY location_key, ts_utc
        HAVING COUNT(*) > 1
    )
    """
    duplicate_count = spark.sql(dup_query).collect()[0]['dup_cnt']
    
    # Check NULL values in key columns
    null_query = f"""
    SELECT 
        SUM(CASE WHEN date_key IS NULL THEN 1 ELSE 0 END) as date_key_nulls,
        SUM(CASE WHEN time_key IS NULL THEN 1 ELSE 0 END) as time_key_nulls,
        SUM(CASE WHEN location_key IS NULL THEN 1 ELSE 0 END) as location_key_nulls
    FROM {silver_table}
    {where_clause}
    """
    null_result = spark.sql(null_query).collect()[0]
    
    validation_result = {
        "total_records": total_records,
        "duplicate_records": duplicate_count,
        "date_key_nulls": null_result['date_key_nulls'],
        "time_key_nulls": null_result['time_key_nulls'],
        "location_key_nulls": null_result['location_key_nulls'],
        "validation_passed": (
            duplicate_count == 0 and
            null_result['date_key_nulls'] == 0 and
            null_result['time_key_nulls'] == 0 and
            null_result['location_key_nulls'] == 0
        )
    }
    
    print(f"Validation results:")
    print(f"  Total records: {validation_result['total_records']:,}")
    print(f"  Duplicates: {validation_result['duplicate_records']}")
    print(f"  NULL date_key: {validation_result['date_key_nulls']}")
    print(f"  NULL time_key: {validation_result['time_key_nulls']}")
    print(f"  NULL location_key: {validation_result['location_key_nulls']}")
    
    if validation_result['validation_passed']:
        print("✓ All validations passed")
    else:
        print("⚠️  WARNING: Validation issues detected!")
    
    return validation_result


@flow(
    name="Silver Transformation Flow",
    description="Transform Bronze to Silver layer with validation",
    log_prints=True
)
def silver_transformation_flow(
    mode: str = "incremental",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    skip_validation: bool = False,
    bronze_table: str = "hadoop_catalog.lh.bronze.open_meteo_hourly",
    silver_table: str = "hadoop_catalog.lh.silver.air_quality_hourly_clean",
    warehouse: str = "hdfs://khoa-master:9000/warehouse/iceberg",
    require_yarn: bool = True
) -> Dict:
    """Silver transformation flow with single SparkSession.
    
    Args:
        mode: 'full' (overwrite) or 'incremental' (merge/upsert)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        skip_validation: Skip validation step
        bronze_table: Source bronze table
        silver_table: Target silver table
        warehouse: Iceberg warehouse URI
        require_yarn: Validate YARN deployment
        
    Returns:
        Dictionary with flow statistics
    """
    print("="*80)
    print("SILVER TRANSFORMATION FLOW")
    print("="*80)
    print(f"Mode: {mode}")
    print(f"Bronze table: {bronze_table}")
    print(f"Silver table: {silver_table}")
    print(f"Warehouse: {warehouse}")
    
    flow_start = time.time()
    
    # Map mode to write strategy
    write_mode = "overwrite" if mode == "full" else "merge"
    
    # Create single Spark session for entire flow
    with get_spark_session(
        app_name="silver_transformation_flow",
        require_yarn=require_yarn
    ) as spark:
        
        # Validate YARN if required
        if require_yarn:
            validate_yarn_mode(spark)
        
        # Log Spark info
        log_spark_info(spark, "Silver Flow")
        
        # Set warehouse
        spark.conf.set("spark.sql.catalog.hadoop_catalog.warehouse", warehouse)
        
        # Execute transformation
        transform_result = transform_bronze_to_silver_task(
            start_date=start_date,
            end_date=end_date,
            write_mode=write_mode,
            bronze_table=bronze_table,
            silver_table=silver_table
        )
        
        # Validate if not skipped and transformation succeeded
        validation_result = None
        if not skip_validation and transform_result.get('status') != 'skipped':
            validation_result = validate_silver_task(
                start_date=start_date,
                end_date=end_date,
                silver_table=silver_table
            )
        
        elapsed = time.time() - flow_start
        
        print(f"\n{'='*80}")
        print("SILVER FLOW COMPLETE")
        print(f"{'='*80}")
        print(f"Records processed: {transform_result.get('records_processed', 0)}")
        print(f"Elapsed time: {elapsed:.1f}s")
        
        return {
            "success": True,
            "mode": mode,
            "transformation": transform_result,
            "validation": validation_result or "skipped",
            "elapsed_seconds": elapsed
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Silver Transformation Prefect Flow - Submit via spark_submit.sh"
    )
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="incremental",
        help="full=overwrite all, incremental=merge/upsert only"
    )
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip validation step"
    )
    parser.add_argument(
        "--bronze-table",
        default="hadoop_catalog.lh.bronze.open_meteo_hourly"
    )
    parser.add_argument(
        "--silver-table",
        default="hadoop_catalog.lh.silver.air_quality_hourly_clean"
    )
    parser.add_argument(
        "--warehouse",
        default=os.getenv("WAREHOUSE_URI", "hdfs://khoa-master:9000/warehouse/iceberg")
    )
    parser.add_argument(
        "--no-yarn-check",
        action="store_true",
        help="Skip YARN validation (for local testing only)"
    )
    
    args = parser.parse_args()
    
    # Run flow
    result = silver_transformation_flow(
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        skip_validation=args.skip_validation,
        bronze_table=args.bronze_table,
        silver_table=args.silver_table,
        warehouse=args.warehouse,
        require_yarn=not args.no_yarn_check
    )
    
    sys.exit(0 if result["success"] else 1)
