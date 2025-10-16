#!/usr/bin/env python3
"""
Test auto-detect new data range for Silver transformation
"""
import sys
import os

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
JOBS_DIR = os.path.join(ROOT_DIR, "jobs")
sys.path.insert(0, SRC_DIR)
sys.path.insert(0, JOBS_DIR)

from pyspark.sql import SparkSession

# Build Spark session
spark = SparkSession.builder \
    .appName("test_auto_detect") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.hadoop_catalog.type", "hadoop") \
    .config("spark.sql.catalog.hadoop_catalog.warehouse", "hdfs://khoa-master:9000/warehouse/iceberg") \
    .getOrCreate()

try:
    # Import function - need to load the file directly
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "transform_bronze_to_silver",
        os.path.join(JOBS_DIR, "silver", "transform_bronze_to_silver.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    get_new_data_range = module.get_new_data_range
    
    bronze_table = "hadoop_catalog.lh.bronze.open_meteo_hourly"
    silver_table = "hadoop_catalog.lh.silver.air_quality_hourly_clean"
    
    print("=" * 80)
    print("TESTING AUTO-DETECT NEW DATA RANGE")
    print("=" * 80)
    print()
    
    # Test auto-detect
    start_date, end_date = get_new_data_range(spark, bronze_table, silver_table)
    
    print()
    print("=" * 80)
    print("RESULT:")
    print("=" * 80)
    
    if start_date == "NO_NEW_DATA":
        print("✓ Silver is up-to-date, no new data to process")
    elif start_date is None:
        print("⊘ Silver is empty or doesn't exist, need full load")
    else:
        print(f"↻ New data range: {start_date} to {end_date}")
        print()
        print("This means Silver flow will only process:")
        print(f"  SELECT * FROM {bronze_table}")
        print(f"  WHERE date_utc BETWEEN '{start_date}' AND '{end_date}'")
        print()
        print("Instead of reading the entire Bronze table! 🚀")
    
    print("=" * 80)
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()
finally:
    spark.stop()
