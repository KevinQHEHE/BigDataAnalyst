"""Generate dim_time with all 24 hours of the day.

This script generates a complete time dimension table with one row for each hour (0-23).
This is a one-time generation script that creates a static reference dimension.

The dimension includes:
- time_key (0, 100, 200, ..., 2300)
- time_value ('00:00', '01:00', ..., '23:00')
- hour (0-23)
- work_shift ('night', 'morning', 'afternoon', 'evening')

Work shift definitions:
- night: 00:00-05:59 (hours 0-5)
- morning: 06:00-11:59 (hours 6-11)
- afternoon: 12:00-17:59 (hours 12-17)
- evening: 18:00-23:59 (hours 18-23)

Usage:
  bash scripts/spark_submit.sh jobs/gold/load_dim_time.py
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
    StructType, StructField, IntegerType, StringType
)


def build_spark_session(app_name: str = "load_dim_time") -> SparkSession:
    """Build Spark session for dimension loading."""
    from lakehouse_aqi import spark_session
    if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME"):
        return spark_session.build(app_name=app_name)
    else:
        return spark_session.build(app_name=app_name, mode="local")


def generate_dim_time(
    spark: SparkSession,
    target_table: str = "hadoop_catalog.lh.gold.dim_time"
) -> int:
    """Generate time dimension for all 24 hours.
    
    Args:
        spark: SparkSession
        target_table: Target dimension table
    
    Returns:
        Number of time records generated (should be 24)
    """
    print("Generating time dimension for 24 hours")
    
    # Define time dimension schema
    time_schema = StructType([
        StructField("time_key", IntegerType(), False),
        StructField("time_value", StringType(), False),
        StructField("hour", IntegerType(), False),
        StructField("work_shift", StringType(), False)
    ])
    
    # Generate time dimension data
    time_data = []
    for hour in range(24):
        time_key = hour * 100  # 0, 100, 200, ..., 2300
        time_value = f"{hour:02d}:00"
        
        # Determine work shift
        if 0 <= hour < 6:
            work_shift = "night"
        elif 6 <= hour < 12:
            work_shift = "morning"
        elif 12 <= hour < 18:
            work_shift = "afternoon"
        else:  # 18 <= hour < 24
            work_shift = "evening"
        
        time_data.append((time_key, time_value, hour, work_shift))
    
    # Create DataFrame
    df_time = spark.createDataFrame(time_data, schema=time_schema)
    
    time_count = df_time.count()
    print(f"Generated {time_count} time records")
    
    # Show preview
    print("\nTime dimension records:")
    df_time.show(24, truncate=False)
    
    # Show distribution by work shift
    print("\nWork shift distribution:")
    df_time.groupBy("work_shift").count().orderBy("work_shift").show()
    
    # Load into dimension table (overwrite mode)
    print(f"\nLoading into table: {target_table}")
    df_time.write.format("iceberg").mode("overwrite").saveAsTable(target_table)
    
    print(f"Successfully loaded {time_count} time records")
    
    return time_count


def main():
    parser = argparse.ArgumentParser(description="Generate dim_time for all 24 hours")
    parser.add_argument(
        "--target-table",
        default="hadoop_catalog.lh.gold.dim_time",
        help="Target dimension table"
    )
    
    args = parser.parse_args()
    
    # Build Spark session
    spark = build_spark_session()
    
    try:
        time_count = generate_dim_time(
            spark=spark,
            target_table=args.target_table
        )
        
        print(f"\n{'='*60}")
        print(f"dim_time generation completed successfully")
        print(f"Time records loaded: {time_count}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"Error generating dim_time: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
