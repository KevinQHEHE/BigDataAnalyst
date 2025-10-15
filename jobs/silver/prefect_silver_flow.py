"""Prefect flow for Silver layer transformation (Bronze → Silver).

This flow orchestrates the transformation from bronze to silver layer with:
- Automatic retry on failure
- Logging and monitoring
- Configurable date ranges
- Idempotent operations

Usage:
  python jobs/silver/prefect_silver_flow.py --date-range 2024-10-01 2024-10-31
  python jobs/silver/prefect_silver_flow.py  # Process all data
"""
import argparse
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner

# Setup paths
ROOT_DIR = Path(__file__).resolve().parent.parent.parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dotenv import load_dotenv
load_dotenv(ROOT_DIR / ".env")


@task(
    name="Transform Bronze to Silver",
    description="Transform bronze data to silver with date_key/time_key enrichment",
    retries=2,
    retry_delay_seconds=60,
    log_prints=True
)
def transform_bronze_to_silver_task(
    start_date: str = None,
    end_date: str = None,
    bronze_table: str = "hadoop_catalog.lh.bronze.open_meteo_hourly",
    silver_table: str = "hadoop_catalog.lh.silver.air_quality_hourly_clean"
) -> dict:
    """Task to transform bronze to silver."""
    from pyspark.sql import SparkSession
    from lakehouse_aqi import spark_session
    
    # Import transform function
    sys.path.insert(0, str(Path(__file__).parent))
    from transform_bronze_to_silver import transform_bronze_to_silver
    
    print(f"Starting Bronze → Silver transformation")
    if start_date and end_date:
        print(f"Date range: {start_date} to {end_date}")
    
    # Build Spark session
    spark = spark_session.build(app_name="prefect_silver_transform")
    
    try:
        result = transform_bronze_to_silver(
            spark=spark,
            bronze_table=bronze_table,
            silver_table=silver_table,
            start_date=start_date,
            end_date=end_date
        )
        
        print(f"Transformation result: {result}")
        return result
        
    finally:
        spark.stop()


@task(
    name="Validate Silver Data",
    description="Validate silver table data quality",
    log_prints=True
)
def validate_silver_task(start_date: str = None, end_date: str = None) -> dict:
    """Task to validate silver data quality."""
    from pyspark.sql import SparkSession
    from lakehouse_aqi import spark_session
    
    spark = spark_session.build(app_name="prefect_silver_validation")
    
    try:
        silver_table = "hadoop_catalog.lh.silver.air_quality_hourly_clean"
        
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
        
        # Check for duplicates
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
            SUM(CASE WHEN time_key IS NULL THEN 1 ELSE 0 END) as time_key_nulls
        FROM {silver_table}
        {where_clause}
        """
        null_result = spark.sql(null_query).collect()[0]
        
        validation_result = {
            "total_records": total_records,
            "duplicate_records": duplicate_count,
            "date_key_nulls": null_result['date_key_nulls'],
            "time_key_nulls": null_result['time_key_nulls'],
            "validation_passed": (
                duplicate_count == 0 and
                null_result['date_key_nulls'] == 0 and
                null_result['time_key_nulls'] == 0
            )
        }
        
        print(f"Validation result: {validation_result}")
        
        if not validation_result['validation_passed']:
            print("⚠️  WARNING: Validation issues detected!")
        else:
            print("✓ All validations passed")
        
        return validation_result
        
    finally:
        spark.stop()


@flow(
    name="Silver Layer Transformation",
    description="Transform Bronze to Silver with validation",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True
)
def silver_transformation_flow(
    start_date: str = None,
    end_date: str = None,
    skip_validation: bool = False
):
    """
    Main flow for silver layer transformation.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        skip_validation: Skip validation step
    """
    print("="*70)
    print("Starting Silver Layer Transformation Flow")
    print("="*70)
    
    # Step 1: Transform bronze to silver
    transform_result = transform_bronze_to_silver_task(
        start_date=start_date,
        end_date=end_date
    )
    
    # Step 2: Validate (if not skipped)
    if not skip_validation and transform_result['status'] == 'success':
        validation_result = validate_silver_task(
            start_date=start_date,
            end_date=end_date
        )
        
        return {
            "transformation": transform_result,
            "validation": validation_result
        }
    
    return {
        "transformation": transform_result,
        "validation": "skipped"
    }


@flow(
    name="Silver Backfill by Month",
    description="Backfill silver data month by month",
    log_prints=True
)
def silver_backfill_flow(
    start_date: str,
    end_date: str,
    skip_validation: bool = False
):
    """
    Backfill flow that processes data month by month.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        skip_validation: Skip validation for each month
    """
    from dateutil.relativedelta import relativedelta
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    current = start
    results = []
    
    while current <= end:
        # Calculate month boundaries
        month_start = current.replace(day=1)
        month_end = (month_start + relativedelta(months=1)) - timedelta(days=1)
        
        # Don't exceed end date
        if month_end > end:
            month_end = end
        
        month_start_str = month_start.strftime("%Y-%m-%d")
        month_end_str = month_end.strftime("%Y-%m-%d")
        
        print(f"\n{'='*70}")
        print(f"Processing month: {month_start_str} to {month_end_str}")
        print(f"{'='*70}")
        
        # Run transformation for this month
        result = silver_transformation_flow(
            start_date=month_start_str,
            end_date=month_end_str,
            skip_validation=skip_validation
        )
        
        results.append({
            "month": month_start.strftime("%Y-%m"),
            "result": result
        })
        
        # Move to next month
        current = month_start + relativedelta(months=1)
    
    print("\n" + "="*70)
    print("Backfill Summary")
    print("="*70)
    for item in results:
        print(f"Month {item['month']}: {item['result']['transformation']['status']}")
    
    return results


def main():
    parser = argparse.ArgumentParser(description="Prefect flow for Silver transformation")
    parser.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        help="Date range to process (YYYY-MM-DD YYYY-MM-DD)"
    )
    parser.add_argument(
        "--backfill-monthly",
        action="store_true",
        help="Process data month by month (useful for large date ranges)"
    )
    parser.add_argument(
        "--skip-validation",
        action="store_true",
        help="Skip validation step"
    )
    
    args = parser.parse_args()
    
    start_date = None
    end_date = None
    
    if args.date_range:
        start_date, end_date = args.date_range
        # Validate dates
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as e:
            print(f"Error: Invalid date format. Use YYYY-MM-DD: {e}")
            sys.exit(1)
    
    # Run appropriate flow
    if args.backfill_monthly and start_date and end_date:
        silver_backfill_flow(
            start_date=start_date,
            end_date=end_date,
            skip_validation=args.skip_validation
        )
    else:
        silver_transformation_flow(
            start_date=start_date,
            end_date=end_date,
            skip_validation=args.skip_validation
        )


if __name__ == "__main__":
    main()
