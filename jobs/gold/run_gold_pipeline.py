"""Gold Layer Pipeline - Load all dimension tables.

This script orchestrates loading all dimension tables in the gold layer:
1. dim_location (from data/locations.jsonl)
2. dim_pollutant (from data/dim_pollutant.jsonl)
3. dim_time (auto-generated for 24 hours)
4. dim_date (extracted from silver layer)

Usage:
  bash scripts/spark_submit.sh jobs/gold/run_gold_pipeline.py -- [OPTIONS]
  
  # Load all dimensions
  bash scripts/spark_submit.sh jobs/gold/run_gold_pipeline.py
  
  # Load specific dimensions only
  bash scripts/spark_submit.sh jobs/gold/run_gold_pipeline.py -- --only location,pollutant
  
  # Skip specific dimensions
  bash scripts/spark_submit.sh jobs/gold/run_gold_pipeline.py -- --skip date
  
  # Custom data file paths
  bash scripts/spark_submit.sh jobs/gold/run_gold_pipeline.py -- \
    --locations /path/to/locations.jsonl \
    --pollutants /path/to/dim_pollutant.jsonl
"""
import argparse
import os
import sys

# Ensure local src is importable
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT_DIR, ".env"))

from pyspark.sql import SparkSession


def build_spark_session(app_name: str = "run_gold_pipeline") -> SparkSession:
    """Build Spark session for gold pipeline."""
    from lakehouse_aqi import spark_session
    if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME"):
        return spark_session.build(app_name=app_name)
    else:
        return spark_session.build(app_name=app_name, mode="local")


def load_dimension_location(spark: SparkSession, locations_path: str) -> int:
    """Load dim_location."""
    from load_dim_location import load_dim_location
    
    print("\n" + "="*60)
    print("Loading dim_location")
    print("="*60)
    
    count = load_dim_location(spark=spark, locations_path=locations_path)
    print(f"✓ dim_location loaded: {count} locations")
    return count


def load_dimension_pollutant(spark: SparkSession, pollutants_path: str) -> int:
    """Load dim_pollutant."""
    from load_dim_pollutant import load_dim_pollutant
    
    print("\n" + "="*60)
    print("Loading dim_pollutant")
    print("="*60)
    
    count = load_dim_pollutant(spark=spark, pollutants_path=pollutants_path)
    print(f"✓ dim_pollutant loaded: {count} pollutants")
    return count


def load_dimension_time(spark: SparkSession) -> int:
    """Generate and load dim_time."""
    from load_dim_time import generate_dim_time
    
    print("\n" + "="*60)
    print("Generating dim_time")
    print("="*60)
    
    count = generate_dim_time(spark=spark)
    print(f"✓ dim_time generated: {count} time records")
    return count


def load_dimension_date(spark: SparkSession) -> int:
    """Generate and load dim_date from silver."""
    from load_dim_date import generate_dim_date
    
    print("\n" + "="*60)
    print("Generating dim_date (from silver layer)")
    print("="*60)
    
    count = generate_dim_date(spark=spark)
    print(f"✓ dim_date generated: {count} dates")
    return count


def main():
    parser = argparse.ArgumentParser(
        description="Run gold layer pipeline: load all dimension tables"
    )
    parser.add_argument(
        "--only",
        default="",
        help="Comma-separated list of dimensions to load (location,pollutant,time,date). If not set, all will be loaded."
    )
    parser.add_argument(
        "--skip",
        default="",
        help="Comma-separated list of dimensions to skip (location,pollutant,time,date)"
    )
    parser.add_argument(
        "--locations",
        default="hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl",
        help="Path to locations.jsonl (HDFS or local with file://)"
    )
    parser.add_argument(
        "--pollutants",
        default="hdfs://khoa-master:9000/user/dlhnhom2/data/dim_pollutant.jsonl",
        help="Path to dim_pollutant.jsonl (HDFS or local with file://)"
    )
    
    args = parser.parse_args()
    
    # Parse only/skip lists
    only_dims = set(dim.strip().lower() for dim in args.only.split(",") if dim.strip())
    skip_dims = set(dim.strip().lower() for dim in args.skip.split(",") if dim.strip())
    
    # Determine which dimensions to load
    all_dims = {"location", "pollutant", "time", "date"}
    if only_dims:
        dims_to_load = only_dims & all_dims
    else:
        dims_to_load = all_dims - skip_dims
    
    if not dims_to_load:
        print("No dimensions to load. Exiting.")
        sys.exit(0)
    
    print(f"Dimensions to load: {', '.join(sorted(dims_to_load))}")
    
    # Build Spark session once for entire pipeline
    spark = build_spark_session()
    
    try:
        results = {}
        
        # Load dim_location
        if "location" in dims_to_load:
            results["dim_location"] = load_dimension_location(
                spark=spark,
                locations_path=args.locations
            )
        
        # Load dim_pollutant
        if "pollutant" in dims_to_load:
            results["dim_pollutant"] = load_dimension_pollutant(
                spark=spark,
                pollutants_path=args.pollutants
            )
        
        # Load dim_time
        if "time" in dims_to_load:
            results["dim_time"] = load_dimension_time(spark=spark)
        
        # Load dim_date (must run last as it depends on silver layer)
        if "date" in dims_to_load:
            results["dim_date"] = load_dimension_date(spark=spark)
        
        # Print final summary
        print("\n" + "="*60)
        print("GOLD PIPELINE COMPLETED SUCCESSFULLY")
        print("="*60)
        for key, value in results.items():
            print(f"  {key}: {value} records")
        print("="*60)
        
    except Exception as e:
        print(f"\n{'='*60}")
        print(f"ERROR: Gold pipeline failed")
        print(f"{'='*60}")
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
