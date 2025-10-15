"""Silver Layer Transformation - Bronze → Silver with date_key/time_key enrichment.

This script transforms data from bronze to silver layer by adding dimensional keys
and standardizing the schema. This is ONLY for silver transformation.

For dimension table loading, use jobs/gold/run_gold_pipeline.py instead.

Usage:
  bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- [OPTIONS]
  
  # Process all bronze data
  bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py
  
  # Process specific date range
  bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- --date-range 2024-01-01 2024-12-31
"""
import argparse
import os
import sys
from datetime import datetime
from typing import List, Optional

# Ensure local src is importable
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT_DIR, ".env"))

from pyspark.sql import SparkSession


def build_spark_session(app_name: str = "run_silver_pipeline") -> SparkSession:
    """Build Spark session for silver pipeline."""
    from lakehouse_aqi import spark_session
    if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME"):
        return spark_session.build(app_name=app_name)
    else:
        return spark_session.build(app_name=app_name, mode="local")


def run_bronze_to_silver(
    spark: SparkSession,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None
) -> int:
    """Run bronze to silver transformation."""
    from transform_bronze_to_silver import transform_bronze_to_silver
    
    print("\n" + "="*60)
    print("Bronze → Silver Transformation")
    print("="*60)
    
    record_count = transform_bronze_to_silver(
        spark=spark,
        start_date=start_date,
        end_date=end_date
    )
    
    print(f"✓ Bronze → Silver completed: {record_count} records processed")
    return record_count


def main():
    parser = argparse.ArgumentParser(
        description="Run silver layer transformation: bronze → silver"
    )
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
    
    # Parse date range
    start_date = None
    end_date = None
    if args.date_range:
        start_date, end_date = args.date_range
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as e:
            print(f"Error: Invalid date format. Use YYYY-MM-DD: {e}")
            sys.exit(1)
    
    # Build Spark session
    spark = build_spark_session()
    
    try:
        # Run Bronze → Silver transformation
        record_count = run_bronze_to_silver(
            spark=spark,
            start_date=start_date,
            end_date=end_date
        )
        
        # Print final summary
        print("\n" + "="*60)
        print("SILVER TRANSFORMATION COMPLETED SUCCESSFULLY")
        print("="*60)
        print(f"  Records processed: {record_count}")
        print("="*60)
        print("\nNext steps:")
        print("  - To load dimension tables, run: spark-submit jobs/gold/run_gold_pipeline.py")
        
    except Exception as e:
        print(f"\n{'='*60}")
        print(f"ERROR: Silver transformation failed")
        print(f"{'='*60}")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
