#!/usr/bin/env python3
"""
Demo: Incremental Processing Auto-Detection

This script demonstrates how auto-detect works for Silver and Gold layers.
It shows how much data would be processed with and without optimization.
"""
import os
import sys

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
sys.path.insert(0, SRC_DIR)

from pyspark.sql import SparkSession
from datetime import datetime

def build_spark():
    """Build Spark session"""
    return SparkSession.builder \
        .appName("demo_auto_detect") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.hadoop_catalog.type", "hadoop") \
        .config("spark.sql.catalog.hadoop_catalog.warehouse", "hdfs://khoa-master:9000/warehouse/iceberg") \
        .getOrCreate()

def check_layer_status(spark, source_table, target_table, layer_name):
    """Check status of a data layer"""
    print(f"\n{'='*80}")
    print(f"{layer_name.upper()} LAYER STATUS")
    print(f"{'='*80}")
    
    # Source table stats
    source_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {source_table}").collect()[0]['cnt']
    source_max = spark.sql(f"SELECT MAX(ts_utc) as max_ts FROM {source_table}").collect()[0]['max_ts']
    
    print(f"\n{layer_name} Source Table: {source_table}")
    print(f"  Total records: {source_count:,}")
    print(f"  Latest timestamp: {source_max}")
    
    # Target table stats
    try:
        target_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {target_table}").collect()[0]['cnt']
        target_max = spark.sql(f"SELECT MAX(ts_utc) as max_ts FROM {target_table}").collect()[0]['max_ts']
        
        print(f"\n{layer_name} Target Table: {target_table}")
        print(f"  Total records: {target_count:,}")
        print(f"  Latest timestamp: {target_max}")
        
        # Check new data
        if source_max and target_max:
            new_records = spark.sql(f"""
                SELECT COUNT(*) as cnt 
                FROM {source_table} 
                WHERE ts_utc > '{target_max}'
            """).collect()[0]['cnt']
            
            print(f"\n📊 Optimization Impact:")
            print(f"  Without auto-detect: Would process {source_count:,} records ❌")
            print(f"  With auto-detect:    Only process {new_records:,} records ✅")
            
            if new_records == 0:
                print(f"  Status: ✓ UP-TO-DATE (would skip processing)")
                saved_pct = 100.0
            else:
                saved_pct = ((source_count - new_records) / source_count) * 100
                print(f"  Savings: {saved_pct:.1f}% fewer records to process")
            
            return {
                "source_count": source_count,
                "target_count": target_count,
                "new_records": new_records,
                "savings_pct": saved_pct
            }
        else:
            print(f"\n⊘ Target table empty, need full load")
            return {"source_count": source_count, "target_count": 0, "new_records": source_count, "savings_pct": 0}
            
    except Exception as e:
        print(f"\n⊘ Target table doesn't exist or error: {e}")
        print(f"  Need full load of {source_count:,} records")
        return {"source_count": source_count, "target_count": 0, "new_records": source_count, "savings_pct": 0}

def main():
    spark = build_spark()
    
    print("="*80)
    print("INCREMENTAL PROCESSING AUTO-DETECTION DEMO")
    print("="*80)
    print(f"Timestamp: {datetime.now().isoformat()}")
    
    try:
        # Check Bronze → Silver
        bronze_table = "hadoop_catalog.lh.bronze.open_meteo_hourly"
        silver_table = "hadoop_catalog.lh.silver.air_quality_hourly_clean"
        
        silver_stats = check_layer_status(spark, bronze_table, silver_table, "Bronze → Silver")
        
        # Check Silver → Gold Hourly
        gold_hourly_table = "hadoop_catalog.lh.gold.fact_air_quality_hourly"
        
        gold_hourly_stats = check_layer_status(spark, silver_table, gold_hourly_table, "Silver → Gold Hourly")
        
        # Check Gold Hourly → Gold Daily
        gold_daily_table = "hadoop_catalog.lh.gold.fact_city_daily"
        
        gold_daily_stats = check_layer_status(spark, gold_hourly_table, gold_daily_table, "Gold Hourly → Gold Daily")
        
        # Overall summary
        print(f"\n{'='*80}")
        print("OVERALL OPTIMIZATION IMPACT")
        print(f"{'='*80}")
        
        total_without = (
            silver_stats['source_count'] + 
            gold_hourly_stats['source_count'] + 
            gold_daily_stats['source_count']
        )
        
        total_with = (
            silver_stats['new_records'] + 
            gold_hourly_stats['new_records'] + 
            gold_daily_stats['new_records']
        )
        
        if total_without > 0:
            overall_savings = ((total_without - total_with) / total_without) * 100
        else:
            overall_savings = 0
        
        print(f"\nWithout auto-detect:")
        print(f"  Silver:      {silver_stats['source_count']:,} records")
        print(f"  Gold Hourly: {gold_hourly_stats['source_count']:,} records")
        print(f"  Gold Daily:  {gold_daily_stats['source_count']:,} records")
        print(f"  TOTAL:       {total_without:,} records")
        
        print(f"\nWith auto-detect:")
        print(f"  Silver:      {silver_stats['new_records']:,} records")
        print(f"  Gold Hourly: {gold_hourly_stats['new_records']:,} records")
        print(f"  Gold Daily:  {gold_daily_stats['new_records']:,} records")
        print(f"  TOTAL:       {total_with:,} records")
        
        print(f"\n🚀 OVERALL SAVINGS: {overall_savings:.1f}%")
        print(f"   Processing {total_with:,} instead of {total_without:,} records")
        
        if overall_savings >= 90:
            print(f"   ⭐ EXCELLENT optimization!")
        elif overall_savings >= 70:
            print(f"   ✅ GOOD optimization")
        elif overall_savings >= 50:
            print(f"   👍 MODERATE optimization")
        else:
            print(f"   ℹ️  Limited optimization (may need full reload)")
        
        print(f"\n{'='*80}")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
