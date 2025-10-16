#!/usr/bin/env python3
"""
Check latest timestamps in Bronze table to understand what --hourly mode would ingest
"""
import sys
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# Iceberg warehouse
WAREHOUSE = "hdfs://khoa-master:9000/warehouse/iceberg"
TABLE = "hadoop_catalog.lh.bronze.open_meteo_hourly"

def main():
    # Create Spark session
    spark = SparkSession.builder \
        .appName("check_bronze_latest") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.catalog.hadoop_catalog.warehouse", WAREHOUSE)
    
    print("=" * 80)
    print("BRONZE TABLE LATEST TIMESTAMPS")
    print("=" * 80)
    
    try:
        # Get latest timestamp per location
        print("\nLatest data per location (first 10):")
        print("-" * 80)
        result = spark.sql(f"""
            SELECT 
                location_key,
                MAX(ts_utc) as latest_dt,
                COUNT(*) as row_count
            FROM {TABLE}
            GROUP BY location_key
            ORDER BY location_key
            LIMIT 10
        """)
        result.show(truncate=False)
        
        # Overall stats
        print("\nOverall Bronze statistics:")
        print("-" * 80)
        overall = spark.sql(f"""
            SELECT 
                MIN(ts_utc) as earliest,
                MAX(ts_utc) as latest,
                COUNT(*) as total_rows,
                COUNT(DISTINCT location_key) as total_locations
            FROM {TABLE}
        """)
        overall.show(truncate=False)
        
        # Show what hourly mode would ingest TODAY
        today = datetime.now().strftime("%Y-%m-%d")
        print(f"\n📅 WHAT --hourly WILL DO (today = {today}):")
        print("=" * 80)
        
        result_list = spark.sql(f"""
            SELECT 
                location_key,
                MAX(ts_utc) as latest_dt
            FROM {TABLE}
            GROUP BY location_key
            ORDER BY location_key
        """).collect()
        
        locations_need_update = 0
        locations_up_to_date = 0
        
        for row in result_list:
            latest = row['latest_dt']
            latest_date = str(latest).split()[0]
            loc_name = row['location_key']
            
            if latest_date >= today:
                print(f"  ✓ {loc_name:30} | Already up-to-date (latest: {latest})")
                locations_up_to_date += 1
            else:
                latest_dt = datetime.strptime(latest_date, "%Y-%m-%d")
                next_day = (latest_dt + timedelta(days=1)).strftime("%Y-%m-%d")
                print(f"  ↻ {loc_name:30} | Will ingest: {next_day} → {today} (latest: {latest})")
                locations_need_update += 1
        
        print("\n" + "=" * 80)
        print(f"Summary:")
        print(f"  Locations need update: {locations_need_update}")
        print(f"  Locations up-to-date:  {locations_up_to_date}")
        print(f"  Total locations:       {len(result_list)}")
        print("=" * 80)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nNote: Make sure Bronze table exists and has data.")
        print(f"Table: {TABLE}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
