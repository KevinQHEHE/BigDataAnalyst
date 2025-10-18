#!/usr/bin/env python3
"""Delete data for a time range from all layers (Bronze, Silver, Gold).

Usage:
    bash scripts/spark_submit.sh scripts/delete_data.py -- --start-date 2025-10-17 --end-date 2025-10-18
"""
import argparse
import sys
from pathlib import Path
from datetime import datetime

# Add src to path
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
sys.path.insert(0, str(SRC_DIR))

from dotenv import load_dotenv
load_dotenv(ROOT_DIR / ".env")

from lakehouse_aqi import spark_session


def validate_date_range(start_date: str, end_date: str):
    """Validate date format and range."""
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start > end:
            raise ValueError("start-date must be <= end-date")
    except ValueError as e:
        print(f"Date validation error: {e}")
        sys.exit(1)


def verify_data_in_range(spark, start_date: str, end_date: str) -> dict:
    """Verify and count records in date range across all layers."""
    counts = {}
    
    # Bronze
    bronze_count = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM hadoop_catalog.lh.bronze.open_meteo_hourly 
        WHERE date_utc >= '{start_date}' AND date_utc <= '{end_date}'
    """).collect()[0]['cnt']
    counts['bronze'] = bronze_count
    
    # Silver
    silver_count = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM hadoop_catalog.lh.silver.air_quality_hourly_clean 
        WHERE date_utc >= '{start_date}' AND date_utc <= '{end_date}'
    """).collect()[0]['cnt']
    counts['silver'] = silver_count
    
    # Gold - fact_hourly
    try:
        fact_hourly_count = spark.sql(f"""
            SELECT COUNT(*) as cnt FROM hadoop_catalog.lh.gold.fact_air_quality_hourly 
            WHERE DATE(ts_utc) >= '{start_date}' AND DATE(ts_utc) <= '{end_date}'
        """).collect()[0]['cnt']
        counts['fact_hourly'] = fact_hourly_count
    except:
        counts['fact_hourly'] = 0
    
    # Gold - fact_daily
    try:
        start_key = int(start_date.replace('-', ''))
        end_key = int(end_date.replace('-', ''))
        fact_daily_count = spark.sql(f"""
            SELECT COUNT(*) as cnt FROM hadoop_catalog.lh.gold.fact_city_daily 
            WHERE date_key >= {start_key} AND date_key <= {end_key}
        """).collect()[0]['cnt']
        counts['fact_daily'] = fact_daily_count
    except:
        counts['fact_daily'] = 0
    
    # Gold - fact_episode
    try:
        fact_episode_count = spark.sql(f"""
            SELECT COUNT(*) as cnt FROM hadoop_catalog.lh.gold.fact_episode 
            WHERE (DATE(start_ts_utc) >= '{start_date}' AND DATE(start_ts_utc) <= '{end_date}')
               OR (DATE(end_ts_utc) >= '{start_date}' AND DATE(end_ts_utc) <= '{end_date}')
        """).collect()[0]['cnt']
        counts['fact_episode'] = fact_episode_count
    except:
        counts['fact_episode'] = 0
    
    print(f"Bronze: {counts['bronze']:,} records")
    print(f"Silver: {counts['silver']:,} records")
    print(f"Gold (fact_hourly): {counts['fact_hourly']:,} records")
    print(f"Gold (fact_daily): {counts['fact_daily']:,} records")
    print(f"Gold (fact_episode): {counts['fact_episode']:,} records")
    
    return counts


def delete_bronze_data(spark, start_date: str, end_date: str) -> int:
    """Delete Bronze layer data for date range."""
    print(f"\n{'='*80}")
    print(f"DELETING BRONZE LAYER: {start_date} to {end_date}")
    print(f"{'='*80}")
    
    table = "hadoop_catalog.lh.bronze.open_meteo_hourly"
    
    # Count before
    before = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM {table} 
        WHERE date_utc >= '{start_date}' AND date_utc <= '{end_date}'
    """).collect()[0]['cnt']
    print(f"Records before delete: {before:,}")
    
    if before == 0:
        print("No records to delete")
        return 0
    
    # Delete
    spark.sql(f"""
        DELETE FROM {table} 
        WHERE date_utc >= '{start_date}' AND date_utc <= '{end_date}'
    """)
    
    # Count after
    after = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM {table} 
        WHERE date_utc >= '{start_date}' AND date_utc <= '{end_date}'
    """).collect()[0]['cnt']
    print(f"Records after delete: {after:,}")
    print(f"Deleted {before - after:,} records from Bronze")
    
    return before - after


def delete_silver_data(spark, start_date: str, end_date: str) -> int:
    """Delete Silver layer data for date range."""
    print(f"\n{'='*80}")
    print(f"DELETING SILVER LAYER: {start_date} to {end_date}")
    print(f"{'='*80}")
    
    table = "hadoop_catalog.lh.silver.air_quality_hourly_clean"
    
    # Count before
    before = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM {table} 
        WHERE date_utc >= '{start_date}' AND date_utc <= '{end_date}'
    """).collect()[0]['cnt']
    print(f"Records before delete: {before:,}")
    
    if before == 0:
        print("No records to delete")
        return 0
    
    # Delete
    spark.sql(f"""
        DELETE FROM {table} 
        WHERE date_utc >= '{start_date}' AND date_utc <= '{end_date}'
    """)
    
    # Count after
    after = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM {table} 
        WHERE date_utc >= '{start_date}' AND date_utc <= '{end_date}'
    """).collect()[0]['cnt']
    print(f"Records after delete: {after:,}")
    print(f"Deleted {before - after:,} records from Silver")
    
    return before - after


def delete_gold_data(spark, start_date: str, end_date: str) -> dict:
    """Delete Gold layer data for date range."""
    print(f"\n{'='*80}")
    print(f"DELETING GOLD LAYER: {start_date} to {end_date}")
    print(f"{'='*80}")
    
    results = {}
    
    # date_key format: YYYYMMDD (for filtering range)
    start_date_key = int(start_date.replace('-', ''))
    end_date_key = int(end_date.replace('-', ''))
    
    # 1. fact_air_quality_hourly
    table1 = "hadoop_catalog.lh.gold.fact_air_quality_hourly"
    try:
        before1 = spark.sql(f"""
            SELECT COUNT(*) as cnt FROM {table1} 
            WHERE DATE(ts_utc) >= '{start_date}' AND DATE(ts_utc) <= '{end_date}'
        """).collect()[0]['cnt']
        print(f"\nfact_air_quality_hourly: {before1:,} records")
        
        if before1 > 0:
            spark.sql(f"""
                DELETE FROM {table1} 
                WHERE DATE(ts_utc) >= '{start_date}' AND DATE(ts_utc) <= '{end_date}'
            """)
            after1 = spark.sql(f"""
                SELECT COUNT(*) as cnt FROM {table1} 
                WHERE DATE(ts_utc) >= '{start_date}' AND DATE(ts_utc) <= '{end_date}'
            """).collect()[0]['cnt']
            results['fact_hourly'] = before1 - after1
            print(f"Deleted {results['fact_hourly']:,} records")
        else:
            results['fact_hourly'] = 0
    except Exception as e:
        print(f"Error deleting fact_hourly: {e}")
        results['fact_hourly'] = 0
    
    # 2. fact_city_daily
    table2 = "hadoop_catalog.lh.gold.fact_city_daily"
    try:
        before2 = spark.sql(f"""
            SELECT COUNT(*) as cnt FROM {table2} 
            WHERE date_key >= {start_date_key} AND date_key <= {end_date_key}
        """).collect()[0]['cnt']
        print(f"\nfact_city_daily: {before2:,} records")
        
        if before2 > 0:
            spark.sql(f"""
                DELETE FROM {table2} 
                WHERE date_key >= {start_date_key} AND date_key <= {end_date_key}
            """)
            after2 = spark.sql(f"""
                SELECT COUNT(*) as cnt FROM {table2} 
                WHERE date_key >= {start_date_key} AND date_key <= {end_date_key}
            """).collect()[0]['cnt']
            results['fact_daily'] = before2 - after2
            print(f"Deleted {results['fact_daily']:,} records")
        else:
            results['fact_daily'] = 0
    except Exception as e:
        print(f"Error deleting fact_daily: {e}")
        results['fact_daily'] = 0
    
    # 3. fact_episode
    table3 = "hadoop_catalog.lh.gold.fact_episode"
    try:
        before3 = spark.sql(f"""
            SELECT COUNT(*) as cnt FROM {table3} 
            WHERE (DATE(start_ts_utc) >= '{start_date}' AND DATE(start_ts_utc) <= '{end_date}')
               OR (DATE(end_ts_utc) >= '{start_date}' AND DATE(end_ts_utc) <= '{end_date}')
        """).collect()[0]['cnt']
        print(f"\nfact_episode: {before3:,} records")
        
        if before3 > 0:
            spark.sql(f"""
                DELETE FROM {table3} 
                WHERE (DATE(start_ts_utc) >= '{start_date}' AND DATE(start_ts_utc) <= '{end_date}')
                   OR (DATE(end_ts_utc) >= '{start_date}' AND DATE(end_ts_utc) <= '{end_date}')
            """)
            after3 = spark.sql(f"""
                SELECT COUNT(*) as cnt FROM {table3} 
                WHERE (DATE(start_ts_utc) >= '{start_date}' AND DATE(start_ts_utc) <= '{end_date}')
                   OR (DATE(end_ts_utc) >= '{start_date}' AND DATE(end_ts_utc) <= '{end_date}')
            """).collect()[0]['cnt']
            results['fact_episode'] = before3 - after3
            print(f"Deleted {results['fact_episode']:,} records")
        else:
            results['fact_episode'] = 0
    except Exception as e:
        print(f"Error deleting fact_episode: {e}")
        results['fact_episode'] = 0
    
    return results


def delete_dim_tables(spark) -> dict:
    """Delete all records from dimension tables."""
    print(f"\n{'='*80}")
    print("DELETING DIMENSION TABLES (Full Mode)")
    print(f"{'='*80}")
    
    results = {}
    
    # 1. dim_date
    table1 = "hadoop_catalog.lh.gold.dim_date"
    try:
        before1 = spark.sql(f"SELECT COUNT(*) as cnt FROM {table1}").collect()[0]['cnt']
        print(f"\ndim_date: {before1:,} records")
        
        if before1 > 0:
            spark.sql(f"DELETE FROM {table1}")
            after1 = spark.sql(f"SELECT COUNT(*) as cnt FROM {table1}").collect()[0]['cnt']
            results['dim_date'] = before1 - after1
            print(f"Deleted {results['dim_date']:,} records")
        else:
            results['dim_date'] = 0
    except Exception as e:
        print(f"Error deleting dim_date: {e}")
        results['dim_date'] = 0
    
    # 2. dim_time
    table2 = "hadoop_catalog.lh.gold.dim_time"
    try:
        before2 = spark.sql(f"SELECT COUNT(*) as cnt FROM {table2}").collect()[0]['cnt']
        print(f"\ndim_time: {before2:,} records")
        
        if before2 > 0:
            spark.sql(f"DELETE FROM {table2}")
            after2 = spark.sql(f"SELECT COUNT(*) as cnt FROM {table2}").collect()[0]['cnt']
            results['dim_time'] = before2 - after2
            print(f"Deleted {results['dim_time']:,} records")
        else:
            results['dim_time'] = 0
    except Exception as e:
        print(f"Error deleting dim_time: {e}")
        results['dim_time'] = 0
    
    # 3. dim_location
    table3 = "hadoop_catalog.lh.gold.dim_location"
    try:
        before3 = spark.sql(f"SELECT COUNT(*) as cnt FROM {table3}").collect()[0]['cnt']
        print(f"\ndim_location: {before3:,} records")
        
        if before3 > 0:
            spark.sql(f"DELETE FROM {table3}")
            after3 = spark.sql(f"SELECT COUNT(*) as cnt FROM {table3}").collect()[0]['cnt']
            results['dim_location'] = before3 - after3
            print(f"Deleted {results['dim_location']:,} records")
        else:
            results['dim_location'] = 0
    except Exception as e:
        print(f"Error deleting dim_location: {e}")
        results['dim_location'] = 0
    
    # 4. dim_pollutant
    table4 = "hadoop_catalog.lh.gold.dim_pollutant"
    try:
        before4 = spark.sql(f"SELECT COUNT(*) as cnt FROM {table4}").collect()[0]['cnt']
        print(f"\ndim_pollutant: {before4:,} records")
        
        if before4 > 0:
            spark.sql(f"DELETE FROM {table4}")
            after4 = spark.sql(f"SELECT COUNT(*) as cnt FROM {table4}").collect()[0]['cnt']
            results['dim_pollutant'] = before4 - after4
            print(f"Deleted {results['dim_pollutant']:,} records")
        else:
            results['dim_pollutant'] = 0
    except Exception as e:
        print(f"Error deleting dim_pollutant: {e}")
        results['dim_pollutant'] = 0
    
    return results


def main():
    parser = argparse.ArgumentParser(
        description="Delete data for a time range from all layers (Bronze, Silver, Gold)"
    )
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date to delete (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date to delete (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--mode",
        default="fact",
        choices=["fact", "full"],
        help="Delete mode: 'fact' (Bronze/Silver/Gold fact tables only) or 'full' (also delete all dim tables)"
    )
    
    args = parser.parse_args()
    start_date = args.start_date
    end_date = args.end_date
    mode = args.mode
    
    # Validate dates
    validate_date_range(start_date, end_date)
    
    print("="*80)
    print(f"DELETE DATA FOR TIME RANGE: {start_date} to {end_date}")
    print("="*80)
    print()
    
    # Build Spark session
    app_name = f"delete_data_{start_date}_to_{end_date}"
    spark = spark_session.build(app_name=app_name)
    
    try:
        # Step 1: Verify data in range before deletion
        before_counts = verify_data_in_range(spark, start_date, end_date)
        before_total = sum(before_counts.values())
    
        if before_total == 0:
            print("\nNo records found in the specified date range. Exiting.")
            return
        
        # Step 2: Delete from all layers
        total_deleted = 0
        
        # Delete Bronze
        bronze_deleted = delete_bronze_data(spark, start_date, end_date)
        total_deleted += bronze_deleted
        
        # Delete Silver
        silver_deleted = delete_silver_data(spark, start_date, end_date)
        total_deleted += silver_deleted
        
        # Delete Gold
        gold_results = delete_gold_data(spark, start_date, end_date)
        gold_deleted = sum(gold_results.values())
        total_deleted += gold_deleted
        
        # Delete Dim tables if mode is 'full'
        dim_deleted = 0
        if mode == "full":
            dim_results = delete_dim_tables(spark)
            dim_deleted = sum(dim_results.values())
            total_deleted += dim_deleted
        
        # Step 3: Verify data after deletion
        print(f"\n{'='*80}")
        print("VERIFYING DATA AFTER DELETION")
        print(f"{'='*80}")
        
        after_counts = verify_data_in_range(spark, start_date, end_date)
        after_total = sum(after_counts.values())
        
        # Summary
        print(f"\n{'='*80}")
        print("DELETION SUMMARY")
        print(f"{'='*80}")
        print(f"Date range: {start_date} to {end_date}")
        print(f"Mode: {mode}")
        print(f"Records before: {before_total:,}")
        print(f"Records deleted: {total_deleted:,}")
        print(f"Records after: {after_total:,}")
        
        if after_total == 0:
            print("Deletion completed successfully. All records removed.")
        else:
            print(f"WARNING: {after_total:,} records still remain in the date range.")
        
        print(f"{'='*80}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
