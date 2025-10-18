"""Generate dim_date from unique dates in silver layer.

This script extracts all unique dates from hadoop_catalog.lh.silver.air_quality_hourly_clean
and generates a complete date dimension with all calendar attributes.

The dimension includes:
- date_key (YYYYMMDD integer)
- date_value (DATE)
- day_of_month (1-31)
- day_of_week (1=Monday, 7=Sunday)
- week_of_year (1-53)
- month (1-12)
- month_name (January, February, ...)
- quarter (1-4)
- year (YYYY)
- is_weekend (TRUE/FALSE)

Modes:
- full: Overwrite entire dimension (rebuild from all silver dates)
- incremental: Merge only new dates not yet in dimension

Usage:
  # Incremental load (default) - only add new dates
  bash scripts/spark_submit.sh jobs/gold/load_dim_date.py -- --mode incremental
  
  # Full refresh - rebuild entire dimension
  bash scripts/spark_submit.sh jobs/gold/load_dim_date.py -- --mode full
  
  # Load from specific silver table
  bash scripts/spark_submit.sh jobs/gold/load_dim_date.py -- \
    --mode incremental \
    --silver-table hadoop_catalog.lh.silver.air_quality_hourly_clean
"""
import argparse
import os
import sys

# Ensure local src is importable
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT_DIR, ".env"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, date_format, dayofmonth, dayofweek, weekofyear,
    month, quarter, year, when, expr
)


def build_spark_session(app_name: str = "load_dim_date") -> SparkSession:
    """Build Spark session for dimension loading."""
    from lakehouse_aqi import spark_session
    if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME"):
        return spark_session.build(app_name=app_name)
    else:
        return spark_session.build(app_name=app_name, mode="local")


def generate_dim_date(
    spark: SparkSession,
    silver_table: str = "hadoop_catalog.lh.silver.air_quality_hourly_clean",
    target_table: str = "hadoop_catalog.lh.gold.dim_date",
    mode: str = "incremental"
) -> int:
    """Generate date dimension from silver layer dates.
    
    Args:
        spark: SparkSession
        silver_table: Source silver table to extract dates from
        target_table: Target dimension table
        mode: 'full' (overwrite) or 'incremental' (merge new dates only)
    
    Returns:
        Number of dates processed
    """
    print(f"Extracting unique dates from: {silver_table}")
    print(f"Mode: {mode}")
    
    # Check if dimension table exists
    table_exists = spark.catalog.tableExists(target_table)
    
    if mode == "incremental" and table_exists:
        # Get dates that already exist in dimension
        existing_dates_query = f"""
            SELECT DISTINCT date_value
            FROM {target_table}
        """
        existing_dates = spark.sql(existing_dates_query)
        existing_count = existing_dates.count()
        print(f"Found {existing_count} existing dates in dimension")
        
        # Get all dates from silver
        all_silver_dates = spark.sql(f"""
            SELECT DISTINCT date_utc
            FROM {silver_table}
            ORDER BY date_utc
        """)
        
        # Find new dates not yet in dimension
        df_dates = all_silver_dates.join(
            existing_dates,
            all_silver_dates.date_utc == existing_dates.date_value,
            "left_anti"
        ).select("date_utc")
        
        date_count = df_dates.count()
        
        if date_count == 0:
            print("No new dates to add. Dimension is up to date.")
            return 0
        
        print(f"Found {date_count} new dates to add")
    else:
        # Full mode or table doesn't exist - extract all dates
        if not table_exists:
            print("Dimension table does not exist. Creating with full mode.")
        
        df_dates = spark.sql(f"""
            SELECT DISTINCT date_utc
            FROM {silver_table}
            ORDER BY date_utc
        """)
        
        date_count = df_dates.count()
        
        if date_count == 0:
            print("No dates found in silver table. Please run bronze->silver transformation first.")
            return 0
        
        print(f"Found {date_count} total dates")
    
    # Generate all date attributes
    df_dim_date = df_dates.withColumn(
        "date_key",
        date_format(col("date_utc"), "yyyyMMdd").cast("int")
    ).withColumn(
        "date_value",
        col("date_utc")
    ).withColumn(
        "day_of_month",
        dayofmonth(col("date_utc"))
    ).withColumn(
        "day_of_week",
        # Spark dayofweek: 1=Sunday, 2=Monday, ..., 7=Saturday
        # Convert to ISO: 1=Monday, 7=Sunday
        when(dayofweek(col("date_utc")) == 1, 7)
        .otherwise(dayofweek(col("date_utc")) - 1)
    ).withColumn(
        "week_of_year",
        weekofyear(col("date_utc"))
    ).withColumn(
        "month",
        month(col("date_utc"))
    ).withColumn(
        "month_name",
        date_format(col("date_utc"), "MMMM")
    ).withColumn(
        "quarter",
        quarter(col("date_utc"))
    ).withColumn(
        "year",
        year(col("date_utc"))
    ).withColumn(
        "is_weekend",
        # day_of_week: 6=Saturday, 7=Sunday
        col("day_of_week").isin(6, 7)
    ).select(
        "date_key",
        "date_value",
        "day_of_month",
        "day_of_week",
        "week_of_year",
        "month",
        "month_name",
        "quarter",
        "year",
        "is_weekend"
    )
    
    # Show preview
    print("\nSample date dimension records:")
    df_dim_date.show(10, truncate=False)
    
    # Show date range summary
    print(f"\nDate range summary:")
    df_dim_date.createOrReplaceTempView('tmp_dates')
    spark.sql("SELECT MIN(date_value) as min_date, MAX(date_value) as max_date, COUNT(*) as total_dates FROM tmp_dates").show()
    
    # Load into dimension table
    print(f"\nLoading into table: {target_table}")
    
    if mode == "full" or not table_exists:
        # Overwrite mode - rebuild entire dimension
        print("Writing in overwrite mode (full refresh)")
        df_dim_date.write.format("iceberg").mode("overwrite").saveAsTable(target_table)
    else:
        # Incremental mode - merge only new dates using staging table
        print("Writing in incremental mode (merge new dates)")
        tmp_table = f"{target_table}_staging"
        df_dim_date.write.format("iceberg").mode("overwrite").saveAsTable(tmp_table)
        
        merge_sql = f"""
        MERGE INTO {target_table} AS target
        USING {tmp_table} AS source
        ON target.date_key = source.date_key
        WHEN NOT MATCHED THEN INSERT *
        """
        spark.sql(merge_sql)
        
        # Cleanup staging table
        spark.sql(f"DROP TABLE IF EXISTS {tmp_table}")
    
    print(f"Successfully loaded {date_count} dates")
    
    # Show final dimension stats
    final_count = spark.sql(f"SELECT COUNT(*) as total FROM {target_table}").collect()[0]['total']
    print(f"Total dates in dimension: {final_count}")
    
    return date_count


def main():
    parser = argparse.ArgumentParser(description="Generate dim_date from silver layer")
    parser.add_argument(
        "--mode",
        choices=["full", "incremental"],
        default="incremental",
        help="Load mode: 'full' (overwrite) or 'incremental' (merge new dates only)"
    )
    parser.add_argument(
        "--silver-table",
        default="hadoop_catalog.lh.silver.air_quality_hourly_clean",
        help="Source silver table to extract dates from"
    )
    parser.add_argument(
        "--target-table",
        default="hadoop_catalog.lh.gold.dim_date",
        help="Target dimension table"
    )
    
    args = parser.parse_args()
    
    # Build Spark session
    spark = build_spark_session()
    
    try:
        date_count = generate_dim_date(
            spark=spark,
            silver_table=args.silver_table,
            target_table=args.target_table,
            mode=args.mode
        )
        
        print(f"\n{'='*60}")
        print(f"dim_date generation completed successfully")
        print(f"Mode: {args.mode}")
        print(f"Dates processed: {date_count}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"Error generating dim_date: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
