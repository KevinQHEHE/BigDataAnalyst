"""Load dim_pollutant from data/dim_pollutant.jsonl into gold layer.

This script reads pollutant metadata from JSONL and loads it into the
hadoop_catalog.lh.gold.dim_pollutant dimension table.

This is a one-time load operation (or refresh when pollutant definitions change).

Usage:
  bash scripts/spark_submit.sh jobs/gold/load_dim_pollutant.py -- [OPTIONS]
  
  # Use default pollutants file
  bash scripts/spark_submit.sh jobs/gold/load_dim_pollutant.py
  
  # Use custom pollutants file
  bash scripts/spark_submit.sh jobs/gold/load_dim_pollutant.py -- --pollutants /path/to/dim_pollutant.jsonl
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
    StructType, StructField, StringType
)


def build_spark_session(app_name: str = "load_dim_pollutant") -> SparkSession:
    """Build Spark session for dimension loading."""
    from lakehouse_aqi import spark_session
    if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME"):
        return spark_session.build(app_name=app_name)
    else:
        return spark_session.build(app_name=app_name, mode="local")


def load_dim_pollutant(
    spark: SparkSession,
    pollutants_path: str,
    target_table: str = "hadoop_catalog.lh.gold.dim_pollutant"
) -> int:
    """Load pollutant dimension from JSONL file.
    
    Args:
        spark: SparkSession
        pollutants_path: Path to dim_pollutant.jsonl file
        target_table: Target dimension table
    
    Returns:
        Number of pollutants loaded
    """
    print(f"Reading pollutants from: {pollutants_path}")
    
    # Define schema for pollutants
    pollutant_schema = StructType([
        StructField("pollutant_code", StringType(), False),
        StructField("display_name", StringType(), False),
        StructField("unit_default", StringType(), False),
        StructField("aqi_timespan", StringType(), True)  # Nullable for non-AQI pollutants
    ])
    
    # Read JSONL file (supports HDFS, local with file://, or S3)
    df_pollutants = spark.read.schema(pollutant_schema).json(pollutants_path)
    
    pollutant_count = df_pollutants.count()
    print(f"Found {pollutant_count} pollutants")
    
    # Show preview
    print("\nPollutants to load:")
    df_pollutants.show(truncate=False)
    
    # Truncate and load (replace all data)
    print(f"\nLoading into table: {target_table}")
    df_pollutants.write.format("iceberg").mode("overwrite").saveAsTable(target_table)
    
    print(f"Successfully loaded {pollutant_count} pollutants")
    
    return pollutant_count


def main():
    parser = argparse.ArgumentParser(description="Load dim_pollutant from dim_pollutant.jsonl")
    parser.add_argument(
        "--pollutants",
        default=os.path.join(os.path.dirname(__file__), "..", "..", "data", "dim_pollutant.jsonl"),
        help="Path to dim_pollutant.jsonl file"
    )
    parser.add_argument(
        "--target-table",
        default="hadoop_catalog.lh.gold.dim_pollutant",
        help="Target dimension table"
    )
    
    args = parser.parse_args()
    
    # Build Spark session
    spark = build_spark_session()
    
    try:
        pollutant_count = load_dim_pollutant(
            spark=spark,
            pollutants_path=args.pollutants,
            target_table=args.target_table
        )
        
        print(f"\n{'='*60}")
        print(f"dim_pollutant load completed successfully")
        print(f"Pollutants loaded: {pollutant_count}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"Error loading dim_pollutant: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
