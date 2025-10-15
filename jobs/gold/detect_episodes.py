"""Detect Air Quality Episodes: Identify sustained high-AQI periods.

This script detects "episodes" - continuous periods where AQI exceeds a threshold
for a minimum duration. Default: AQI ≥ 151 for ≥ 4 consecutive hours.

An episode is a run of consecutive hours where:
- AQI >= threshold (default 151, "Unhealthy" level)
- Duration >= min_hours (default 4)

Episodes are written to hadoop_catalog.lh.gold.fact_episode with:
- episode_id (UUID)
- location_key
- start_ts_utc, end_ts_utc
- duration_hours
- peak_aqi (max AQI during episode)
- hours_flagged (count of hours in episode)
- dominant_pollutant (pollutant with highest AQI during episode)
- rule_code (e.g., "AQI>=151_4h")

Usage:
  bash scripts/spark_submit.sh jobs/gold/detect_episodes.py -- [OPTIONS]
  
  # Detect episodes with default threshold (AQI >= 151, min 4h)
  bash scripts/spark_submit.sh jobs/gold/detect_episodes.py -- --mode full
  
  # Custom threshold
  bash scripts/spark_submit.sh jobs/gold/detect_episodes.py -- \
    --mode full \
    --aqi-threshold 101 \
    --min-hours 6
  
  # Process specific date range
  bash scripts/spark_submit.sh jobs/gold/detect_episodes.py -- \
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

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col, when, lag, lead, row_number, sum as _sum, min as _min, max as _max,
    count as _count, first, last, expr, lit, unix_timestamp, date_format
)


def build_spark_session(app_name: str = "detect_episodes") -> SparkSession:
    from lakehouse_aqi import spark_session
    mode = "cluster" if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME") else "local"
    spark = spark_session.build(app_name=app_name, mode=mode)
    
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
    return spark


def detect_episodes(
    spark: SparkSession,
    hourly_table: str = "hadoop_catalog.lh.gold.fact_air_quality_hourly",
    episode_table: str = "hadoop_catalog.lh.gold.fact_episode",
    aqi_threshold: int = 151,
    min_hours: int = 4,
    start_date: str = None,
    end_date: str = None,
    mode: str = "overwrite"
) -> dict:
    """Detect air quality episodes from hourly data.
    
    Args:
        spark: SparkSession
        hourly_table: Source hourly fact table
        episode_table: Target episode fact table
        aqi_threshold: AQI threshold for episodes (default 151)
        min_hours: Minimum consecutive hours (default 4)
        start_date: Optional start date filter (YYYY-MM-DD)
        end_date: Optional end date filter (YYYY-MM-DD)
        mode: Write mode - "overwrite" or "merge"
    
    Returns:
        Dictionary with processing metrics
    """
    start_time = datetime.now()
    
    print(f"Reading from hourly fact table: {hourly_table}")
    print(f"Episode detection: AQI >= {aqi_threshold} for >= {min_hours} hours")
    print(f"Write mode: {mode}")
    
    # Read hourly data, sorted by location and time
    query = f"""
        SELECT 
            location_key,
            ts_utc,
            date_utc,
            aqi,
            dominant_pollutant
        FROM {hourly_table}
        WHERE aqi IS NOT NULL
    """
    
    if start_date and end_date:
        query += f" AND date_utc BETWEEN '{start_date}' AND '{end_date}'"
    elif start_date:
        query += f" AND date_utc >= '{start_date}'"
    elif end_date:
        query += f" AND date_utc <= '{end_date}'"
    
    query += " ORDER BY location_key, ts_utc"
    
    df_hourly = spark.sql(query)
    
    record_count = df_hourly.count()
    if record_count == 0:
        print("No data to process")
        return {
            "status": "skipped",
            "episodes_detected": 0,
            "duration_seconds": 0
        }
    
    print(f"Processing {record_count} hourly records")
    
    # Flag hours that exceed threshold
    df_flagged = df_hourly.withColumn(
        "flagged",
        when(col("aqi") >= aqi_threshold, 1).otherwise(0)
    )
    
    # Detect run boundaries using window functions
    # For each location, identify changes from 0->1 (run start) and 1->0 (run end)
    window_spec = Window.partitionBy("location_key").orderBy("ts_utc")
    
    df_runs = df_flagged.withColumn(
        "prev_flagged",
        lag("flagged", 1).over(window_spec)
    ).withColumn(
        "is_run_start",
        when((col("flagged") == 1) & ((col("prev_flagged").isNull()) | (col("prev_flagged") == 0)), 1).otherwise(0)
    )
    
    # Assign run_id by cumulative sum of run_start
    df_runs = df_runs.withColumn(
        "run_id",
        _sum("is_run_start").over(window_spec.rowsBetween(Window.unboundedPreceding, 0))
    )
    
    # Filter only flagged hours and group by location + run_id
    df_runs_flagged = df_runs.filter(col("flagged") == 1)
    
    # Aggregate each run
    df_episodes = df_runs_flagged.groupBy("location_key", "run_id").agg(
        _min("ts_utc").alias("start_ts_utc"),
        _max("ts_utc").alias("end_ts_utc"),
        _min("date_utc").alias("start_date_utc"),
        _max("aqi").alias("peak_aqi"),
        _count("*").alias("hours_flagged"),
        first("dominant_pollutant", ignorenulls=True).alias("dominant_pollutant")
    )
    
    # Calculate duration in hours (add 1 because start and end are inclusive)
    # For consecutive hours: end - start + 1
    df_episodes = df_episodes.withColumn(
        "duration_hours",
        ((unix_timestamp("end_ts_utc") - unix_timestamp("start_ts_utc")) / 3600.0).cast("int") + 1
    )
    
    # Filter runs that meet minimum duration
    df_episodes = df_episodes.filter(col("duration_hours") >= min_hours)
    
    episode_count = df_episodes.count()
    print(f"Detected {episode_count} episodes (>= {min_hours} hours)")
    
    if episode_count == 0:
        print("No episodes detected")
        return {
            "status": "success",
            "episodes_detected": 0,
            "duration_seconds": (datetime.now() - start_time).total_seconds()
        }
    
    # Add metadata
    rule_code = f"AQI>={aqi_threshold}_{min_hours}h"
    
    df_episodes = df_episodes.withColumn("episode_id", expr("uuid()"))
    df_episodes = df_episodes.withColumn("rule_code", lit(rule_code))
    
    # Select columns in schema order
    df_episodes = df_episodes.select(
        col("episode_id"),
        col("location_key"),
        col("start_ts_utc"),
        col("end_ts_utc"),
        col("start_date_utc"),
        col("duration_hours"),
        col("peak_aqi"),
        col("hours_flagged"),
        col("dominant_pollutant"),
        col("rule_code")
    )
    
    # Show sample episodes
    print("\nSample detected episodes:")
    df_episodes.orderBy(col("peak_aqi").desc()).show(10, truncate=False)
    
    # Write to episode table
    if mode == "overwrite":
        print(f"Overwriting episode table: {episode_table}")
        df_episodes.write.format("iceberg").mode("overwrite").saveAsTable(episode_table)
    else:
        print(f"Merging into episode table: {episode_table}")
        tmp_view = "__tmp_episodes"
        df_episodes.createOrReplaceTempView(tmp_view)
        
        merge_sql = f"""
        MERGE INTO {episode_table} AS target
        USING {tmp_view} AS source
        ON target.location_key = source.location_key 
           AND target.start_ts_utc = source.start_ts_utc
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(merge_sql)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print(f"Successfully detected {episode_count} episodes")
    
    return {
        "status": "success",
        "episodes_detected": episode_count,
        "duration_seconds": duration,
        "mode": mode,
        "aqi_threshold": aqi_threshold,
        "min_hours": min_hours
    }


def main():
    parser = argparse.ArgumentParser(description="Detect Air Quality Episodes")
    parser.add_argument("--mode", choices=["full", "incremental"], default="incremental")
    parser.add_argument("--start-date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--aqi-threshold", type=int, default=151, help="AQI threshold (default: 151)")
    parser.add_argument("--min-hours", type=int, default=4, help="Minimum consecutive hours (default: 4)")
    parser.add_argument("--hourly-table", default="hadoop_catalog.lh.gold.fact_air_quality_hourly")
    parser.add_argument("--episode-table", default="hadoop_catalog.lh.gold.fact_episode")
    
    args = parser.parse_args()
    
    write_mode = "overwrite" if args.mode == "full" else "merge"
    
    spark = build_spark_session()
    
    try:
        result = detect_episodes(
            spark=spark,
            hourly_table=args.hourly_table,
            episode_table=args.episode_table,
            aqi_threshold=args.aqi_threshold,
            min_hours=args.min_hours,
            start_date=args.start_date,
            end_date=args.end_date,
            mode=write_mode
        )
        
        print(f"\n{'='*60}")
        print(f"Episode Detection completed")
        print(f"Status: {result['status']}")
        print(f"Episodes detected: {result['episodes_detected']}")
        print(f"Threshold: AQI >= {result.get('aqi_threshold', 'N/A')}, Min hours: {result.get('min_hours', 'N/A')}")
        print(f"Duration: {result['duration_seconds']:.2f}s")
        print(f"{'='*60}")
        
        sys.exit(0)
        
    except Exception as e:
        print(f"Error during episode detection: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
