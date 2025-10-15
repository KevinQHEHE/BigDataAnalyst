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


def transform_fact_daily(
    spark: SparkSession,
    hourly_table: str = "hadoop_catalog.lh.gold.fact_air_quality_hourly",
    daily_table: str = "hadoop_catalog.lh.gold.fact_city_daily",
    start_date: str = None,
    end_date: str = None,
    mode: str = "overwrite"
) -> dict:
    """Aggregate hourly facts to daily city facts.
    
    Args:
        spark: SparkSession
        hourly_table: Source hourly fact table
        daily_table: Target daily fact table
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        mode: Write mode - "overwrite" or "merge"
    
    Returns:
        Dictionary with processing metrics
    """
    start_time = datetime.now()
    
    print(f"Reading from hourly fact table: {hourly_table}")
    print(f"Write mode: {mode}")
    
    # Read hourly data
    query = f"SELECT * FROM {hourly_table}"
    if start_date and end_date:
        query += f" WHERE date_utc BETWEEN '{start_date}' AND '{end_date}'"
    elif start_date:
        query += f" WHERE date_utc >= '{start_date}'"
    elif end_date:
        query += f" WHERE date_utc <= '{end_date}'"
    
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
    
    # Deduplicate (should be 1 row per location/date already)
    df_daily = df_daily.dropDuplicates(["location_key", "date_utc"])
    
    daily_count = df_daily.count()
    print(f"Generated {daily_count} daily records")
    
    # Write to daily fact table
    if mode == "overwrite":
        print(f"Overwriting daily fact table: {daily_table}")
        df_daily.write.format("iceberg").mode("overwrite").saveAsTable(daily_table)
    else:
        print(f"Merging into daily fact table: {daily_table}")
        tmp_view = "__tmp_fact_daily"
        df_daily.createOrReplaceTempView(tmp_view)
        
        merge_sql = f"""
        MERGE INTO {daily_table} AS target
        USING {tmp_view} AS source
        ON target.location_key = source.location_key 
           AND target.date_utc = source.date_utc
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(merge_sql)
    
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
