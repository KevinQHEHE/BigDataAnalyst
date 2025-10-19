"""Transform Gold Hourly → Gold Daily: Aggregate daily AQI metrics and category hours.

This script reads from hadoop_catalog.lh.gold.fact_air_quality_hourly and aggregates to
hadoop_catalog.lh.gold.fact_city_daily with:
- aqi_daily_max: MAX(aqi) per (location, date)
- dominant_pollutant_daily: pollutant with highest max AQI
- hours_in_cat_*: count hours in each AQI category (Good, Moderate, USG, Unhealthy, Very Unhealthy, Hazardous)
- hours_measured: count non-null AQI hours
- data_completeness: % of hours with data (0-100%)

AQI Categories (US EPA):
- Good: 0-50
- Moderate: 51-100
- Unhealthy for Sensitive Groups (USG): 101-150
- Unhealthy: 151-200
- Very Unhealthy: 201-300
- Hazardous: 301-500

Usage:
  bash scripts/spark_submit.sh jobs/gold/transform_fact_daily.py -- [OPTIONS]
  
  # Process all data (full refresh)
  bash scripts/spark_submit.sh jobs/gold/transform_fact_daily.py -- --mode full
  
  # Process specific date range
  bash scripts/spark_submit.sh jobs/gold/transform_fact_daily.py -- \
    --mode incremental \
    --start-date 2024-01-01 \
    --end-date 2024-12-31
"""
import argparse
import os
import sys
from datetime import datetime

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
sys.path.insert(0, SRC_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT_DIR, ".env"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, max as _max, count as _count, sum as _sum, when, 
    expr, lit, first, date_format
)


def build_spark_session(app_name: str = "transform_fact_daily") -> SparkSession:
    from lakehouse_aqi import spark_session
    mode = "cluster" if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME") else "local"
    spark = spark_session.build(app_name=app_name, mode=mode)
    
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    return spark


def get_new_data_range(
    spark: SparkSession,
    hourly_table: str,
    daily_table: str
) -> tuple:
    """Auto-detect date range of new data in hourly fact that's not in daily fact yet.
    
    Returns:
        (start_date, end_date): Date strings in 'YYYY-MM-DD' format
        (None, None): If daily table is empty (need full load)
        ("NO_NEW_DATA", "NO_NEW_DATA"): If no new data
    """
    try:
        # Check if daily table exists
        daily_exists = spark.catalog.tableExists(daily_table)
        if not daily_exists:
            print(f"Daily table {daily_table} doesn't exist, will process all hourly data")
            return None, None
        
        # Get max date_key in daily table
        daily_max_row = spark.sql(f"""
            SELECT MAX(date_key) as max_date_key 
            FROM {daily_table}
        """).collect()
        
        if not daily_max_row or daily_max_row[0]['max_date_key'] is None:
            print("Daily table is empty, will process all hourly data")
            return None, None
        
        daily_max_date_key = daily_max_row[0]['max_date_key']
        print(f"Latest daily date_key: {daily_max_date_key}")
        
        # Get min/max dates in hourly where date_key > daily_max
        hourly_new_row = spark.sql(f"""
            SELECT 
                MIN(date_utc) as min_date,
                MAX(date_utc) as max_date,
                COUNT(*) as count
            FROM {hourly_table}
            WHERE date_key > {daily_max_date_key}
        """).collect()
        
        if not hourly_new_row or hourly_new_row[0]['min_date'] is None:
            print("No new data in hourly facts, daily is up-to-date")
            return "NO_NEW_DATA", "NO_NEW_DATA"
        
        min_date = str(hourly_new_row[0]['min_date'])
        max_date = str(hourly_new_row[0]['max_date'])
        new_count = hourly_new_row[0]['count']
        
        print(f"Found {new_count} new hourly records: {min_date} to {max_date}")
        return min_date, max_date
        
    except Exception as e:
        print(f"Error detecting new data range: {e}")
        print("  Will process all hourly data")
        return None, None


def transform_fact_daily(
    spark: SparkSession,
    hourly_table: str = "hadoop_catalog.lh.gold.fact_air_quality_hourly",
    daily_table: str = "hadoop_catalog.lh.gold.fact_city_daily",
    start_date: str = None,
    end_date: str = None,
    mode: str = "overwrite",
    auto_detect: bool = True  # Auto-detect new data range if start_date not provided
) -> dict:
    """Aggregate hourly facts to daily city facts.
    
    Args:
        spark: SparkSession
        hourly_table: Source hourly fact table
        daily_table: Target daily fact table
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        mode: Write mode - "overwrite" or "merge"
        auto_detect: Auto-detect new data range if True and start_date not provided
    
    Returns:
        Dictionary with processing metrics
    """
    start_time = datetime.now()
    
    print(f"Reading from hourly fact table: {hourly_table}")
    print(f"Write mode: {mode}")
    
    # Auto-detect new data range for merge mode
    if auto_detect and mode == "merge" and not start_date:
        print("\nAuto-detecting new data range...")
        start_date, end_date = get_new_data_range(spark, hourly_table, daily_table)
        
        if start_date == "NO_NEW_DATA":
            return {
                "status": "skipped",
                "reason": "no_new_data",
                "records_processed": 0,
                "duration_seconds": 0
            }
    
    # Read hourly data
    query = f"SELECT * FROM {hourly_table}"
    if start_date and end_date:
        query += f" WHERE date_utc BETWEEN '{start_date}' AND '{end_date}'"
        print(f"Date range: {start_date} to {end_date}")
    elif start_date:
        query += f" WHERE date_utc >= '{start_date}'"
        print(f"Start date: {start_date}")
    elif end_date:
        query += f" WHERE date_utc <= '{end_date}'"
        print(f"End date: {end_date}")
    else:
        print("Processing all hourly data")
    
    df_hourly = spark.sql(query)
    
    record_count = df_hourly.count()
    if record_count == 0:
        print("No data to process in hourly fact table")
        return {
            "status": "skipped",
            "records_processed": 0,
            "duration_seconds": 0
        }
    
    print(f"Processing {record_count} hourly records")
    
    # Categorize each hour by AQI
    df_categorized = df_hourly.withColumn(
        "cat_good", when((col("aqi") >= 0) & (col("aqi") <= 50), 1).otherwise(0)
    ).withColumn(
        "cat_moderate", when((col("aqi") >= 51) & (col("aqi") <= 100), 1).otherwise(0)
    ).withColumn(
        "cat_usg", when((col("aqi") >= 101) & (col("aqi") <= 150), 1).otherwise(0)
    ).withColumn(
        "cat_unhealthy", when((col("aqi") >= 151) & (col("aqi") <= 200), 1).otherwise(0)
    ).withColumn(
        "cat_very_unhealthy", when((col("aqi") >= 201) & (col("aqi") <= 300), 1).otherwise(0)
    ).withColumn(
        "cat_hazardous", when(col("aqi") >= 301, 1).otherwise(0)
    ).withColumn(
        "has_aqi", when(col("aqi").isNotNull(), 1).otherwise(0)
    )
    
    # Aggregate by location and date
    # US EPA standard: daily AQI = max hourly AQI
    df_daily = df_categorized.groupBy("location_key", "date_utc", "date_key").agg(
        _max("aqi").alias("aqi_daily_max"),
        first("dominant_pollutant", ignorenulls=True).alias("dominant_pollutant_daily"),
        _sum("cat_good").alias("hours_in_cat_good"),
        _sum("cat_moderate").alias("hours_in_cat_moderate"),
        _sum("cat_usg").alias("hours_in_cat_usg"),
        _sum("cat_unhealthy").alias("hours_in_cat_unhealthy"),
        _sum("cat_very_unhealthy").alias("hours_in_cat_very_unhealthy"),
        _sum("cat_hazardous").alias("hours_in_cat_hazardous"),
        _sum("has_aqi").alias("hours_measured")
    )
    
    # Calculate data completeness (24 hours in a day)
    df_daily = df_daily.withColumn(
        "data_completeness",
        (col("hours_measured") / lit(24.0) * 100.0)
    )
    
    # Add daily_record_id
    df_daily = df_daily.withColumn("daily_record_id", expr("uuid()"))
    
    # Select columns in schema order
    df_daily = df_daily.select(
        col("daily_record_id"),
        col("location_key"),
        col("date_utc"),
        col("date_key"),
        col("aqi_daily_max"),
        col("dominant_pollutant_daily"),
        col("hours_in_cat_good"),
        col("hours_in_cat_moderate"),
        col("hours_in_cat_usg"),
        col("hours_in_cat_unhealthy"),
        col("hours_in_cat_very_unhealthy"),
        col("hours_in_cat_hazardous"),
        col("hours_measured"),
        col("data_completeness")
    )
    
    daily_count = df_daily.count()
    print(f"Generated {daily_count} daily records")
    
    # Deduplicate (should be 1 row per location/date already, but ensure no duplicates)
    from pyspark.sql import Window
    from pyspark.sql.functions import row_number
    
    window_spec = Window.partitionBy("location_key", "date_utc").orderBy(col("date_utc").desc())
    df_daily = df_daily.withColumn("_row_num", row_number().over(window_spec)) \
                       .filter(col("_row_num") == 1) \
                       .drop("_row_num")
    
    # Write to daily fact table
    if mode == "overwrite":
        print(f"Overwriting daily fact table: {daily_table}")
        df_daily.write.format("iceberg").mode("overwrite").saveAsTable(daily_table)
    else:
        print(f"Merging into daily fact table: {daily_table}")
        # Materialize dedupped data as temp table instead of view to avoid Spark 4.0 bug
        tmp_table = f"{daily_table}_staging"
        df_daily.write.format("iceberg").mode("overwrite").saveAsTable(tmp_table)
        
        merge_sql = f"""
        MERGE INTO {daily_table} AS target
        USING {tmp_table} AS source
        ON target.location_key = source.location_key 
           AND target.date_utc = source.date_utc
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(merge_sql)
        
        # Cleanup staging table
        spark.sql(f"DROP TABLE IF EXISTS {tmp_table}")
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"Successfully processed {daily_count} daily records")
    
    return {
        "status": "success",
        "records_processed": daily_count,
        "duration_seconds": duration,
        "mode": mode
    }

def main():
    parser = argparse.ArgumentParser(description="Transform Gold Hourly → Gold Daily")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--hourly-table", default="hadoop_catalog.lh.gold.fact_air_quality_hourly")
    parser.add_argument("--daily-table", default="hadoop_catalog.lh.gold.fact_city_daily")
    
    args = parser.parse_args()
    
    write_mode = "overwrite" if args.mode == "full" else "merge"
    
    spark = build_spark_session()
    
    try:
        result = transform_fact_daily(
            spark=spark,
            hourly_table=args.hourly_table,
            daily_table=args.daily_table,
            start_date=args.start_date,
            end_date=args.end_date,
            mode=write_mode
        )
        
        print(f"\n{'='*60}")
        print(f"Gold Daily Fact transformation completed")
        print(f"Status: {result['status']}")
        print(f"Records processed: {result['records_processed']}")
        print(f"Duration: {result['duration_seconds']:.2f}s")
        print(f"{'='*60}")
        
        sys.exit(0)
        
    except Exception as e:
        print(f"Error during transformation: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
