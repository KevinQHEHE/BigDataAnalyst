"""Load dim_location from data/locations.jsonl into gold layer.

This script reads location metadata from JSONL and loads it into the
hadoop_catalog.lh.gold.dim_location dimension table.

This is a one-time load operation (or refresh when locations change).

Usage:
  bash scripts/spark_submit.sh jobs/gold/load_dim_location.py -- [OPTIONS]
  
  # Use default locations file
  bash scripts/spark_submit.sh jobs/gold/load_dim_location.py
  
  # Use custom locations file
  bash scripts/spark_submit.sh jobs/gold/load_dim_location.py -- --locations /path/to/locations.jsonl
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
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)


def build_spark_session(app_name: str = "load_dim_location") -> SparkSession:
    """Build Spark session for dimension loading."""
    from lakehouse_aqi import spark_session
    if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME"):
        return spark_session.build(app_name=app_name)
    else:
        return spark_session.build(app_name=app_name, mode="local")


def load_dim_location(
    spark: SparkSession,
    locations_path: str,
    target_table: str = "hadoop_catalog.lh.gold.dim_location"
) -> int:
    """Load location dimension from JSONL file.
    
    Args:
        spark: SparkSession
        locations_path: Path to locations.jsonl file
        target_table: Target dimension table
    
    Returns:
        Number of locations loaded
    """
    print(f"Reading locations from: {locations_path}")
    
    # Define schema for locations
    location_schema = StructType([
        StructField("location_key", StringType(), False),
        StructField("location_name", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("timezone", StringType(), False)
    ])
    
    # Read JSONL file (supports HDFS, local with file://, or S3)
    df_locations = spark.read.schema(location_schema).json(locations_path)
    
    location_count = df_locations.count()
    print(f"Found {location_count} locations")
    
    # Show preview
    print("\nLocations to load:")
    df_locations.show(truncate=False)
    
    # Truncate and load (replace all data)
    print(f"\nLoading into table: {target_table}")
    df_locations.write.format("iceberg").mode("overwrite").saveAsTable(target_table)
    
    print(f"Successfully loaded {location_count} locations")
    
    return location_count


def main():
    parser = argparse.ArgumentParser(description="Load dim_location from locations.jsonl")
    parser.add_argument(
        "--locations",
        default=os.path.join(os.path.dirname(__file__), "..", "..", "data", "locations.jsonl"),
        help="Path to locations.jsonl file"
    )
    parser.add_argument(
        "--target-table",
        default="hadoop_catalog.lh.gold.dim_location",
        help="Target dimension table"
    )
    
    args = parser.parse_args()
    
    # Build Spark session
    spark = build_spark_session()
    
    try:
        location_count = load_dim_location(
            spark=spark,
            locations_path=args.locations,
            target_table=args.target_table
        )
        
        print(f"\n{'='*60}")
        print(f"dim_location load completed successfully")
        print(f"Locations loaded: {location_count}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"Error loading dim_location: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
