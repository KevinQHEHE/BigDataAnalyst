"""Verify bronze table data."""
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from lakehouse_aqi import spark_session

spark = spark_session.build("verify_bronze")

print("\n" + "="*70)
print("BRONZE TABLE VERIFICATION")
print("="*70)

print("\n📊 Total rows:")
spark.sql("SELECT COUNT(*) as cnt FROM hadoop_catalog.lh.bronze.open_meteo_hourly").show()

print("\n📍 Rows per location:")
spark.sql("""
    SELECT location_key, COUNT(*) as cnt 
    FROM hadoop_catalog.lh.bronze.open_meteo_hourly 
    GROUP BY location_key 
    ORDER BY location_key
""").show()

print("\n📅 Rows per date:")
spark.sql("""
    SELECT date_utc, COUNT(*) as cnt 
    FROM hadoop_catalog.lh.bronze.open_meteo_hourly 
    GROUP BY date_utc 
    ORDER BY date_utc
""").show(50)

print("\n🔍 Check duplicates (location_key, ts_utc):")
result = spark.sql("""
    SELECT location_key, ts_utc, COUNT(*) as dup 
    FROM hadoop_catalog.lh.bronze.open_meteo_hourly 
    GROUP BY location_key, ts_utc 
    HAVING COUNT(*) > 1 
    ORDER BY dup DESC
    LIMIT 50
""")
dup_count = result.count()
if dup_count > 0:
    print(f"⚠️  Found {dup_count} duplicate records!")
    result.show(50, truncate=False)
else:
    print("✓ No duplicates found")

print("\n📈 Date range:")
spark.sql("""
    SELECT MIN(ts_utc) as min_ts, MAX(ts_utc) as max_ts 
    FROM hadoop_catalog.lh.bronze.open_meteo_hourly
""").show(truncate=False)

print("\n" + "="*70)
spark.stop()
