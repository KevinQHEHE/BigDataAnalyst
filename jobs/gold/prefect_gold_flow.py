"""Prefect flow for Gold layer dimension loading.

This flow orchestrates loading of all dimension tables with:
- Parallel execution where possible
- Automatic retry on failure
- Selective loading with --only/--skip
- Aggregated metrics

Usage:
  python jobs/gold/prefect_gold_flow.py
  python jobs/gold/prefect_gold_flow.py --only location pollutant
  python jobs/gold/prefect_gold_flow.py --skip time
"""
import argparse
import sys
from pathlib import Path
from typing import List, Optional

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
    name="Load Dimension Location",
    description="Load location dimension from HDFS",
    retries=2,
    retry_delay_seconds=30,
    log_prints=True
)
def load_dim_location_task() -> dict:
    """Task to load location dimension."""
    from pyspark.sql import SparkSession
    from lakehouse_aqi import spark_session
    
    sys.path.insert(0, str(Path(__file__).parent))
    from load_dim_location import load_dim_location
    
    print("Loading dimension: Location")
    
    spark = spark_session.build(app_name="prefect_dim_location")
    
    try:
        result = load_dim_location(spark)
        print(f"Location dimension result: {result}")
        return result
    finally:
        spark.stop()


@task(
    name="Load Dimension Pollutant",
    description="Load pollutant dimension from HDFS",
    retries=2,
    retry_delay_seconds=30,
    log_prints=True
)
def load_dim_pollutant_task() -> dict:
    """Task to load pollutant dimension."""
    from pyspark.sql import SparkSession
    from lakehouse_aqi import spark_session
    
    sys.path.insert(0, str(Path(__file__).parent))
    from load_dim_pollutant import load_dim_pollutant
    
    print("Loading dimension: Pollutant")
    
    spark = spark_session.build(app_name="prefect_dim_pollutant")
    
    try:
        result = load_dim_pollutant(spark)
        print(f"Pollutant dimension result: {result}")
        return result
    finally:
        spark.stop()


@task(
    name="Load Dimension Time",
    description="Auto-generate time dimension (24 hours)",
    retries=2,
    retry_delay_seconds=30,
    log_prints=True
)
def load_dim_time_task() -> dict:
    """Task to load time dimension."""
    from pyspark.sql import SparkSession
    from lakehouse_aqi import spark_session
    
    sys.path.insert(0, str(Path(__file__).parent))
    from load_dim_time import load_dim_time
    
    print("Loading dimension: Time")
    
    spark = spark_session.build(app_name="prefect_dim_time")
    
    try:
        result = load_dim_time(spark)
        print(f"Time dimension result: {result}")
        return result
    finally:
        spark.stop()


@task(
    name="Load Dimension Date",
    description="Extract date dimension from silver layer",
    retries=2,
    retry_delay_seconds=30,
    log_prints=True
)
def load_dim_date_task() -> dict:
    """Task to load date dimension."""
    from pyspark.sql import SparkSession
    from lakehouse_aqi import spark_session
    
    sys.path.insert(0, str(Path(__file__).parent))
    from load_dim_date import load_dim_date
    
    print("Loading dimension: Date")
    
    spark = spark_session.build(app_name="prefect_dim_date")
    
    try:
        result = load_dim_date(spark)
        print(f"Date dimension result: {result}")
        return result
    finally:
        spark.stop()


@flow(
    name="Gold Dimension Loading",
    description="Load all gold dimension tables with parallel execution",
    task_runner=ConcurrentTaskRunner(),
    log_prints=True
)
def gold_dimension_flow(
    only: Optional[List[str]] = None,
    skip: Optional[List[str]] = None
):
    """
    Main flow for gold dimension loading.
    
    Args:
        only: Only load these dimensions (location, pollutant, time, date)
        skip: Skip these dimensions
    """
    print("="*70)
    print("Starting Gold Dimension Loading Flow")
    print("="*70)
    
    # Define all available dimensions
    all_dimensions = ["location", "pollutant", "time", "date"]
    
    # Determine which dimensions to load
    if only:
        dimensions_to_load = [d for d in only if d in all_dimensions]
        print(f"Loading only: {dimensions_to_load}")
    elif skip:
        dimensions_to_load = [d for d in all_dimensions if d not in skip]
        print(f"Skipping: {skip}")
        print(f"Loading: {dimensions_to_load}")
    else:
        dimensions_to_load = all_dimensions
        print(f"Loading all dimensions: {dimensions_to_load}")
    
    results = {}
    
    # Phase 1: Load location and pollutant in parallel (independent)
    location_future = None
    pollutant_future = None
    
    if "location" in dimensions_to_load:
        location_future = load_dim_location_task.submit()
    
    if "pollutant" in dimensions_to_load:
        pollutant_future = load_dim_pollutant_task.submit()
    
    # Wait for Phase 1 to complete
    if location_future:
        results["location"] = location_future.result()
    
    if pollutant_future:
        results["pollutant"] = pollutant_future.result()
    
    # Phase 2: Load time (independent)
    if "time" in dimensions_to_load:
        time_result = load_dim_time_task()
        results["time"] = time_result
    
    # Phase 3: Load date (depends on silver data)
    if "date" in dimensions_to_load:
        date_result = load_dim_date_task()
        results["date"] = date_result
    
    # Calculate summary
    total_records = sum(
        r.get("records_loaded", r.get("records_generated", 0))
        for r in results.values()
    )
    successful = sum(1 for r in results.values() if r.get("status") == "success")
    
    summary = {
        "dimensions_loaded": len(results),
        "successful": successful,
        "failed": len(results) - successful,
        "total_records": total_records,
        "results": results
    }
    
    print("\n" + "="*70)
    print("Gold Dimension Loading Summary")
    print("="*70)
    print(f"Dimensions processed: {summary['dimensions_loaded']}")
    print(f"Successful: {summary['successful']}")
    print(f"Failed: {summary['failed']}")
    print(f"Total records: {summary['total_records']}")
    print()
    
    for dim_name, result in results.items():
        status_icon = "✓" if result.get("status") == "success" else "✗"
        count = result.get("records_loaded", result.get("records_generated", 0))
        print(f"  {status_icon} {dim_name}: {count} records")
    
    return summary


@flow(
    name="Full Silver-Gold Pipeline",
    description="Complete pipeline from bronze to silver to gold",
    log_prints=True
)
def full_pipeline_flow(
    start_date: str = None,
    end_date: str = None,
    skip_validation: bool = False,
    skip_gold: bool = False
):
    """
    Full pipeline flow: Bronze → Silver → Gold.
    
    Args:
        start_date: Start date for silver transformation
        end_date: End date for silver transformation
        skip_validation: Skip silver validation
        skip_gold: Skip gold dimension loading
    """
    print("="*70)
    print("Starting Full Pipeline: Bronze → Silver → Gold")
    print("="*70)
    
    # Import silver flow
    from jobs.silver.prefect_silver_flow import silver_transformation_flow
    
    # Step 1: Silver transformation
    silver_result = silver_transformation_flow(
        start_date=start_date,
        end_date=end_date,
        skip_validation=skip_validation
    )
    
    # Step 2: Gold dimensions (if not skipped and silver was successful)
    gold_result = None
    if not skip_gold and silver_result['transformation']['status'] == 'success':
        gold_result = gold_dimension_flow()
    
    return {
        "silver": silver_result,
        "gold": gold_result or "skipped"
    }


def main():
    parser = argparse.ArgumentParser(description="Prefect flow for Gold dimensions")
    parser.add_argument(
        "--only",
        nargs="+",
        choices=["location", "pollutant", "time", "date"],
        help="Only load these dimensions"
    )
    parser.add_argument(
        "--skip",
        nargs="+",
        choices=["location", "pollutant", "time", "date"],
        help="Skip these dimensions"
    )
    parser.add_argument(
        "--full-pipeline",
        action="store_true",
        help="Run full pipeline (Bronze → Silver → Gold)"
    )
    parser.add_argument(
        "--date-range",
        nargs=2,
        metavar=("START", "END"),
        help="Date range for silver transformation (YYYY-MM-DD YYYY-MM-DD)"
    )
    
    args = parser.parse_args()
    
    if args.full_pipeline:
        start_date = None
        end_date = None
        if args.date_range:
            start_date, end_date = args.date_range
        
        full_pipeline_flow(
            start_date=start_date,
            end_date=end_date
        )
    else:
        gold_dimension_flow(
            only=args.only,
            skip=args.skip
        )


if __name__ == "__main__":
    main()
