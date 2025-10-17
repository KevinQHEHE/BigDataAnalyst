#!/usr/bin/env python3
"""Delete data for specific date from all layers (Bronze, Silver, Gold).

Usage:
    bash scripts/spark_submit.sh scripts/delete_data.py -- --date 2025-10-17
"""
import argparse
import sys
from pathlib import Path

# Add src to path
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
sys.path.insert(0, str(SRC_DIR))

from dotenv import load_dotenv
load_dotenv(ROOT_DIR / ".env")

from lakehouse_aqi import spark_session


def delete_bronze_data(spark, date: str) -> int:
    """Delete Bronze layer data for specific date."""
    print(f"\n{'='*80}")
    print(f"DELETING BRONZE LAYER: {date}")
    print(f"{'='*80}")
    
    table = "hadoop_catalog.lh.bronze.open_meteo_hourly"
    
    # Count before
    before = spark.sql(f"SELECT COUNT(*) as cnt FROM {table} WHERE date_utc = '{date}'").collect()[0]['cnt']
    print(f"Records before delete: {before:,}")
    
    if before == 0:
        print("No records to delete")
        return 0
    
    # Delete
    spark.sql(f"DELETE FROM {table} WHERE date_utc = '{date}'")
    
    # Count after
    after = spark.sql(f"SELECT COUNT(*) as cnt FROM {table} WHERE date_utc = '{date}'").collect()[0]['cnt']
    print(f"Records after delete: {after:,}")
    print(f"✓ Deleted {before - after:,} records from Bronze")
    
    return before - after


def delete_silver_data(spark, date: str) -> int:
    """Delete Silver layer data for specific date."""
    print(f"\n{'='*80}")
    print(f"DELETING SILVER LAYER: {date}")
    print(f"{'='*80}")
    
    table = "hadoop_catalog.lh.silver.air_quality_hourly_clean"
    
    # Count before
    before = spark.sql(f"SELECT COUNT(*) as cnt FROM {table} WHERE date_utc = '{date}'").collect()[0]['cnt']
    print(f"Records before delete: {before:,}")
    
    if before == 0:
        print("No records to delete")
        return 0
    
    # Delete
    spark.sql(f"DELETE FROM {table} WHERE date_utc = '{date}'")
    
    # Count after
    after = spark.sql(f"SELECT COUNT(*) as cnt FROM {table} WHERE date_utc = '{date}'").collect()[0]['cnt']
    print(f"Records after delete: {after:,}")
    print(f"✓ Deleted {before - after:,} records from Silver")
    
    return before - after


def delete_gold_data(spark, date: str) -> dict:
    """Delete Gold layer data for specific date."""
    print(f"\n{'='*80}")
    print(f"DELETING GOLD LAYER: {date}")
    print(f"{'='*80}")
    
    results = {}
    
    # date_key format: YYYYMMDD
    date_key = int(date.replace('-', ''))
    
    # 1. fact_air_quality_hourly
    table1 = "hadoop_catalog.lh.gold.fact_air_quality_hourly"
    try:
        before1 = spark.sql(f"SELECT COUNT(*) as cnt FROM {table1} WHERE DATE(ts_utc) = '{date}'").collect()[0]['cnt']
        print(f"\nfact_air_quality_hourly: {before1:,} records")
        
        if before1 > 0:
            spark.sql(f"DELETE FROM {table1} WHERE DATE(ts_utc) = '{date}'")
            after1 = spark.sql(f"SELECT COUNT(*) as cnt FROM {table1} WHERE DATE(ts_utc) = '{date}'").collect()[0]['cnt']
            results['fact_hourly'] = before1 - after1
            print(f"✓ Deleted {results['fact_hourly']:,} records")
        else:
            results['fact_hourly'] = 0
    except Exception as e:
        print(f"✗ Error deleting fact_hourly: {e}")
        results['fact_hourly'] = 0
    
    # 2. fact_city_daily
    table2 = "hadoop_catalog.lh.gold.fact_city_daily"
    try:
        before2 = spark.sql(f"SELECT COUNT(*) as cnt FROM {table2} WHERE date_key = {date_key}").collect()[0]['cnt']
        print(f"\nfact_city_daily: {before2:,} records")
        
        if before2 > 0:
            spark.sql(f"DELETE FROM {table2} WHERE date_key = {date_key}")
            after2 = spark.sql(f"SELECT COUNT(*) as cnt FROM {table2} WHERE date_key = {date_key}").collect()[0]['cnt']
            results['fact_daily'] = before2 - after2
            print(f"✓ Deleted {results['fact_daily']:,} records")
        else:
            results['fact_daily'] = 0
    except Exception as e:
        print(f"✗ Error deleting fact_daily: {e}")
        results['fact_daily'] = 0
    
    # 3. fact_episode
    table3 = "hadoop_catalog.lh.gold.fact_episode"
    try:
        before3 = spark.sql(f"""
            SELECT COUNT(*) as cnt FROM {table3} 
            WHERE DATE(start_ts) = '{date}' OR DATE(end_ts) = '{date}'
        """).collect()[0]['cnt']
        print(f"\nfact_episode: {before3:,} records")
        
        if before3 > 0:
            spark.sql(f"""
                DELETE FROM {table3} 
                WHERE DATE(start_ts) = '{date}' OR DATE(end_ts) = '{date}'
            """)
            after3 = spark.sql(f"""
                SELECT COUNT(*) as cnt FROM {table3} 
                WHERE DATE(start_ts) = '{date}' OR DATE(end_ts) = '{date}'
            """).collect()[0]['cnt']
            results['fact_episode'] = before3 - after3
            print(f"✓ Deleted {results['fact_episode']:,} records")
        else:
            results['fact_episode'] = 0
    except Exception as e:
        print(f"✗ Error deleting fact_episode: {e}")
        results['fact_episode'] = 0
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Delete data for specific date from all layers"
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Date to delete (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--layers",
        default="bronze,silver,gold",
        help="Layers to delete from (comma-separated: bronze,silver,gold)"
    )
    
    args = parser.parse_args()
    date = args.date
    layers = [l.strip().lower() for l in args.layers.split(',')]
    
    print("="*80)
    print(f"DELETE DATA FOR DATE: {date}")
    print("="*80)
    print(f"Layers: {', '.join(layers)}")
    print()
    
    # Build Spark session
    spark = spark_session.build(app_name=f"delete_data_{date}")
    
    try:
        total_deleted = 0
        
        # Delete Bronze
        if 'bronze' in layers:
            bronze_deleted = delete_bronze_data(spark, date)
            total_deleted += bronze_deleted
        
        # Delete Silver
        if 'silver' in layers:
            silver_deleted = delete_silver_data(spark, date)
            total_deleted += silver_deleted
        
        # Delete Gold
        if 'gold' in layers:
            gold_results = delete_gold_data(spark, date)
            gold_deleted = sum(gold_results.values())
            total_deleted += gold_deleted
        
        # Summary
        print(f"\n{'='*80}")
        print("DELETION SUMMARY")
        print(f"{'='*80}")
        print(f"Date: {date}")
        print(f"Total records deleted: {total_deleted:,}")
        print(f"{'='*80}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
