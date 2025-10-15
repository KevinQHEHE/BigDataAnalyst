"""Drop bronze table to start fresh."""
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from lakehouse_aqi import spark_session

spark = spark_session.build("cleanup_bronze")

print("Current row count:")
try:
    result = spark.sql("SELECT COUNT(*) as cnt FROM hadoop_catalog.lh.bronze.open_meteo_hourly").collect()
    print(f"  Rows: {result[0]['cnt']}")
except Exception as e:
    print(f"  Table not found or error: {e}")

print("\nDropping table...")
spark.sql("DROP TABLE IF EXISTS hadoop_catalog.lh.bronze.open_meteo_hourly")
print("Table dropped - will be recreated on next insert\n")

spark.stop()
