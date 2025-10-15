"""Ingest air quality data from Open-Meteo API to Bronze table.

This script is designed for both standalone execution and Prefect orchestration.

Modes:
1. BACKFILL: Historical data ingestion with configurable date range
2. UPSERT: Incremental update from latest timestamp in table

Features:
- Chunking: Split large date ranges into smaller chunks to respect rate limits
- Rate limiting: Configurable delay between API calls
- Deduplication: Prevent duplicate records by checking existing data
- YARN optimized: Single Spark session, efficient DataFrame operations
- Memory efficient: Process chunks iteratively, write incrementally
- Prefect-ready: Core functions return structured data for flow orchestration

Usage:
  # Backfill historical data
  spark-submit ingest/bronze/ingest_bronze.py --mode backfill \\
    --start-date 2024-01-01 --end-date 2024-12-31

  # Upsert from latest timestamp
  spark-submit ingest/bronze/ingest_bronze.py --mode upsert

  # Override existing data (use with caution!)
  spark-submit ingest/bronze/ingest_bronze.py --mode backfill \\
    --start-date 2024-01-01 --end-date 2024-01-31 --override

  # Custom locations file
  spark-submit ingest/bronze/ingest_bronze.py --mode upsert \\
    --locations /path/to/locations.jsonl
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

# Ensure local src is importable
# When running via spark-submit, __file__ might be in a temp directory
# So we use absolute path or CWD as fallback
if os.path.exists(os.path.join(os.path.dirname(__file__), "..", "..")):
    ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
else:
    # Fallback to current working directory (when submitted via YARN)
    ROOT_DIR = os.getcwd()

SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT_DIR, ".env"))

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType, DateType
)

# API client imports
try:
    import openmeteo_requests
    import pandas as pd
    import requests_cache
    from retry_requests import retry
except ImportError as e:
    print(f"ERROR: Missing required packages. Install with:")
    print("  pip install openmeteo-requests pandas requests-cache retry-requests")
    sys.exit(1)


# Bronze schema - matches open_meteo_hourly table
BRONZE_SCHEMA = StructType([
    StructField("location_key", StringType(), False),
    StructField("ts_utc", TimestampType(), False),
    StructField("date_utc", DateType(), False),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("aqi", IntegerType(), True),
    StructField("aqi_pm25", IntegerType(), True),
    StructField("aqi_pm10", IntegerType(), True),
    StructField("aqi_no2", IntegerType(), True),
    StructField("aqi_o3", IntegerType(), True),
    StructField("aqi_so2", IntegerType(), True),
    StructField("aqi_co", IntegerType(), True),
    StructField("pm25", DoubleType(), True),
    StructField("pm10", DoubleType(), True),
    StructField("o3", DoubleType(), True),
    StructField("no2", DoubleType(), True),
    StructField("so2", DoubleType(), True),
    StructField("co", DoubleType(), True),
    StructField("aod", DoubleType(), True),
    StructField("dust", DoubleType(), True),
    StructField("uv_index", DoubleType(), True),
    StructField("co2", DoubleType(), True),
    StructField("model_domain", StringType(), True),
    StructField("request_timezone", StringType(), True),
    StructField("_ingested_at", TimestampType(), True),
])


def build_spark_session(app_name: str = "ingest_bronze") -> SparkSession:
    """Build Spark session for bronze ingestion."""
    from lakehouse_aqi import spark_session
    if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME"):
        return spark_session.build(app_name=app_name)
    else:
        return spark_session.build(app_name=app_name, mode="local")


def load_locations(path: str) -> List[Dict]:
    """Load locations from JSON or JSONL file.
    
    Supports:
    - JSON array: [{"location_key": "hanoi", ...}, ...]
    - JSONL: one JSON object per line
    - JSON object: {"locations": [...]}
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Locations file not found: {path}")
    
    with open(path, "r", encoding="utf-8") as f:
        content = f.read().strip()
    
    # JSONL format (one JSON per line)
    if content.startswith('{') and '\n' in content:
        locations = []
        for line in content.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                locations.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        if locations:
            return locations
    
    # JSON format
    try:
        data = json.loads(content)
        if isinstance(data, list):
            return data
        elif isinstance(data, dict) and "locations" in data:
            return data["locations"]
        else:
            raise ValueError(f"Invalid JSON format in {path}")
    except json.JSONDecodeError as e:
        raise ValueError(f"Failed to parse {path}: {e}")


def get_latest_timestamp(
    spark: SparkSession, 
    table: str, 
    location_key: str
) -> Optional[datetime]:
    """Get the latest timestamp for a location from bronze table."""
    try:
        result = spark.sql(f"""
            SELECT MAX(ts_utc) as max_ts 
            FROM {table} 
            WHERE location_key = '{location_key}'
        """).collect()
        
        if result and result[0]["max_ts"]:
            return result[0]["max_ts"]
        return None
    except Exception as e:
        print(f"  ℹ Table {table} not found or empty, starting from scratch")
        return None


def check_data_exists(
    spark: SparkSession,
    table: str,
    location_key: str,
    start_date: str,
    end_date: str
) -> bool:
    """Check if data already exists for location and date range."""
    try:
        count = spark.sql(f"""
            SELECT COUNT(*) as cnt
            FROM {table}
            WHERE location_key = '{location_key}'
              AND date_utc BETWEEN '{start_date}' AND '{end_date}'
        """).collect()[0]["cnt"]
        
        return count > 0
    except Exception:
        return False


def fetch_openmeteo_data(
    latitude: float,
    longitude: float,
    start_date: str,
    end_date: str,
    timezone: str = "UTC"
) -> Optional[pd.DataFrame]:
    """Fetch air quality data from Open-Meteo API.
    
    Args:
        latitude: Location latitude
        longitude: Location longitude
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        timezone: Timezone for data (default: UTC)
    
    Returns:
        DataFrame with hourly data or None if API call fails
    """
    # Setup API client with cache and retry
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": [
            "pm2_5", "pm10", "nitrogen_dioxide", "ozone", 
            "sulphur_dioxide", "carbon_monoxide",
            "aerosol_optical_depth", "dust", "uv_index", "carbon_dioxide",
            "us_aqi", "us_aqi_pm2_5", "us_aqi_pm10", 
            "us_aqi_nitrogen_dioxide", "us_aqi_ozone",
            "us_aqi_sulphur_dioxide", "us_aqi_carbon_monoxide"
        ],
        "timezone": timezone,
        "start_date": start_date,
        "end_date": end_date,
    }
    
    try:
        responses = openmeteo.weather_api(url, params=params)
        response = responses[0]
        
        # Process hourly data
        hourly = response.Hourly()
        
        # Create DataFrame with timezone-aware timestamps
        hourly_data = {
            "ts_utc": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            ),
            "pm25": hourly.Variables(0).ValuesAsNumpy(),
            "pm10": hourly.Variables(1).ValuesAsNumpy(),
            "no2": hourly.Variables(2).ValuesAsNumpy(),
            "o3": hourly.Variables(3).ValuesAsNumpy(),
            "so2": hourly.Variables(4).ValuesAsNumpy(),
            "co": hourly.Variables(5).ValuesAsNumpy(),
            "aod": hourly.Variables(6).ValuesAsNumpy(),
            "dust": hourly.Variables(7).ValuesAsNumpy(),
            "uv_index": hourly.Variables(8).ValuesAsNumpy(),
            "co2": hourly.Variables(9).ValuesAsNumpy(),
            "aqi": hourly.Variables(10).ValuesAsNumpy(),
            "aqi_pm25": hourly.Variables(11).ValuesAsNumpy(),
            "aqi_pm10": hourly.Variables(12).ValuesAsNumpy(),
            "aqi_no2": hourly.Variables(13).ValuesAsNumpy(),
            "aqi_o3": hourly.Variables(14).ValuesAsNumpy(),
            "aqi_so2": hourly.Variables(15).ValuesAsNumpy(),
            "aqi_co": hourly.Variables(16).ValuesAsNumpy(),
        }
        
        df = pd.DataFrame(data=hourly_data)
        
        # Add metadata
        df["latitude"] = response.Latitude()
        df["longitude"] = response.Longitude()
        df["model_domain"] = "CAMS"  # Default model
        df["request_timezone"] = timezone
        
        # Convert timestamp to date for partitioning
        df["date_utc"] = df["ts_utc"].dt.date
        
        return df
        
    except Exception as e:
        print(f"  ✗ API call failed: {e}")
        return None


def generate_date_chunks(
    start_date: str, 
    end_date: str, 
    chunk_days: int = 90
) -> List[Tuple[str, str]]:
    """Split date range into chunks to respect API limits.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        chunk_days: Days per chunk (default: 90, Open-Meteo limit is ~1 year)
    
    Returns:
        List of (chunk_start, chunk_end) tuples
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    chunks = []
    current = start
    
    while current <= end:
        chunk_end = min(current + timedelta(days=chunk_days - 1), end)
        chunks.append((
            current.strftime("%Y-%m-%d"),
            chunk_end.strftime("%Y-%m-%d")
        ))
        current = chunk_end + timedelta(days=1)
    
    return chunks


def ingest_location_chunk(
    spark: SparkSession,
    location: Dict,
    start_date: str,
    end_date: str,
    table: str,
    override: bool = False,
    rate_limit_seconds: float = 1.0
) -> Tuple[int, int]:
    """Ingest data for one location and one time chunk.
    
    Returns:
        (rows_inserted, rows_skipped)
    """
    location_key = location.get("location_key") or location.get("id") or location.get("name")
    location_name = location.get("location_name") or location.get("name", "Unknown")
    latitude = location.get("latitude")
    longitude = location.get("longitude")
    # Always use UTC to avoid timezone date shift issues
    timezone = "UTC"
    
    print(f"\n  📍 {location_name} ({location_key})")
    print(f"     Date range: {start_date} to {end_date}")
    
    # Check if data exists
    if not override:
        if check_data_exists(spark, table, location_key, start_date, end_date):
            row_estimate = (datetime.strptime(end_date, "%Y-%m-%d") - 
                          datetime.strptime(start_date, "%Y-%m-%d")).days * 24
            print(f"     ⏭ Data exists, skipping ({row_estimate} rows)")
            return (0, row_estimate)
    
    # Fetch data from API
    print(f"     🌐 Fetching from Open-Meteo API...")
    pdf = fetch_openmeteo_data(latitude, longitude, start_date, end_date, timezone)
    
    if pdf is None or pdf.empty:
        print(f"     ✗ No data returned")
        return (0, 0)
    
    # Add location_key
    pdf["location_key"] = location_key
    
    # Add ingestion timestamp
    pdf["_ingested_at"] = pd.Timestamp.now(tz="UTC")
    
    # Ensure proper data types
    pdf["ts_utc"] = pd.to_datetime(pdf["ts_utc"], utc=True)
    pdf["date_utc"] = pd.to_datetime(pdf["date_utc"])
    
    # Convert integer fields (AQI values) - API returns floats
    int_columns = ["aqi", "aqi_pm25", "aqi_pm10", "aqi_no2", "aqi_o3", "aqi_so2", "aqi_co"]
    for col in int_columns:
        if col in pdf.columns:
            # Convert to Int64 (nullable integer) to handle NaN properly
            pdf[col] = pdf[col].round().astype('Int64')
    
    # Convert NaN to None for Spark compatibility
    pdf = pdf.where(pdf.notnull(), None)
    
    # Reorder columns to match BRONZE_SCHEMA exactly
    column_order = [field.name for field in BRONZE_SCHEMA.fields]
    pdf = pdf[column_order]
    
    # Convert to Spark DataFrame
    print(f"     💾 Writing {len(pdf)} rows to {table}...")
    sdf = spark.createDataFrame(pdf, schema=BRONZE_SCHEMA)
    
    # Deduplicate by (location_key, ts_utc) - keep first occurrence
    sdf = sdf.dropDuplicates(["location_key", "ts_utc"])
    
    # Cache before counting to avoid consuming the DataFrame
    sdf.cache()
    actual_rows = sdf.count()
    
    if actual_rows < len(pdf):
        print(f"     ℹ Deduped: {len(pdf)} → {actual_rows} rows")
    
    # Write to table (append mode)
    try:
        if override:
            # Delete existing data for this location and date range
            # Since we now use UTC timezone, no need for ±1 day buffer
            spark.sql(f"""
                DELETE FROM {table}
                WHERE location_key = '{location_key}'
                  AND date_utc BETWEEN '{start_date}' AND '{end_date}'
            """)
            print(f"     🗑 Deleted existing data: {start_date} to {end_date}")
        
        sdf.write \
            .format("iceberg") \
            .mode("append") \
            .save(table)
        
        print(f"     ✓ Inserted {actual_rows} rows")
        
        # Unpersist cache
        sdf.unpersist()
        
        # Rate limiting
        if rate_limit_seconds > 0:
            time.sleep(rate_limit_seconds)
        
        return (actual_rows, 0)
        
    except Exception as e:
        print(f"     ✗ Write failed: {e}")
        return (0, 0)


def run_backfill(
    spark: SparkSession,
    locations: List[Dict],
    start_date: str,
    end_date: str,
    table: str,
    chunk_days: int = 90,
    override: bool = False,
    rate_limit: float = 1.0
) -> Dict[str, int]:
    """Run backfill mode: ingest historical data for date range.
    
    Returns:
        Stats dict with total_rows, total_skipped, total_chunks
    """
    print("\n" + "=" * 70)
    print("🔄 BACKFILL MODE")
    print("=" * 70)
    print(f"Date range: {start_date} to {end_date}")
    print(f"Locations: {len(locations)}")
    print(f"Chunk size: {chunk_days} days")
    print(f"Override: {override}")
    print(f"Rate limit: {rate_limit}s between requests")
    print("=" * 70)
    
    # Generate date chunks
    chunks = generate_date_chunks(start_date, end_date, chunk_days)
    print(f"\n📅 Split into {len(chunks)} chunks:")
    for i, (cs, ce) in enumerate(chunks, 1):
        print(f"   Chunk {i}: {cs} to {ce}")
    
    # Process each location × chunk
    total_rows = 0
    total_skipped = 0
    total_chunks = len(locations) * len(chunks)
    completed_chunks = 0
    
    for location in locations:
        loc_name = location.get("location_name") or location.get("name", "Unknown")
        print(f"\n{'─' * 70}")
        print(f"📍 Processing: {loc_name}")
        print(f"{'─' * 70}")
        
        for chunk_start, chunk_end in chunks:
            rows, skipped = ingest_location_chunk(
                spark=spark,
                location=location,
                start_date=chunk_start,
                end_date=chunk_end,
                table=table,
                override=override,
                rate_limit_seconds=rate_limit
            )
            
            total_rows += rows
            total_skipped += skipped
            completed_chunks += 1
            
            # Progress
            pct = (completed_chunks / total_chunks) * 100
            print(f"\n   Progress: {completed_chunks}/{total_chunks} chunks ({pct:.1f}%)")
    
    return {
        "total_rows": total_rows,
        "total_skipped": total_skipped,
        "total_chunks": total_chunks
    }


def run_upsert(
    spark: SparkSession,
    locations: List[Dict],
    table: str,
    lookback_days: int = 7,
    rate_limit: float = 1.0
) -> Dict[str, int]:
    """Run upsert mode: update from latest timestamp to now.
    
    Args:
        lookback_days: Days to look back from latest timestamp (for gap filling)
    
    Returns:
        Stats dict with total_rows, total_skipped
    """
    print("\n" + "=" * 70)
    print("🔄 UPSERT MODE")
    print("=" * 70)
    print(f"Locations: {len(locations)}")
    print(f"Lookback: {lookback_days} days")
    print(f"Rate limit: {rate_limit}s between requests")
    print("=" * 70)
    
    total_rows = 0
    total_skipped = 0
    today = datetime.now().strftime("%Y-%m-%d")
    
    for location in locations:
        loc_key = location.get("location_key") or location.get("id") or location.get("name")
        loc_name = location.get("location_name") or location.get("name", "Unknown")
        
        print(f"\n{'─' * 70}")
        print(f"📍 {loc_name} ({loc_key})")
        print(f"{'─' * 70}")
        
        # Get latest timestamp
        latest_ts = get_latest_timestamp(spark, table, loc_key)
        
        if latest_ts:
            # Start from lookback_days before latest timestamp
            start = (latest_ts - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
            print(f"  Latest data: {latest_ts}")
            print(f"  Fetching from: {start} (with {lookback_days}-day lookback)")
        else:
            # No data exists, start from 7 days ago
            start = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            print(f"  No existing data, fetching last 7 days from {start}")
        
        # Ingest from start to today
        rows, skipped = ingest_location_chunk(
            spark=spark,
            location=location,
            start_date=start,
            end_date=today,
            table=table,
            override=True,  # Always override in upsert mode
            rate_limit_seconds=rate_limit
        )
        
        total_rows += rows
        total_skipped += skipped
    
    return {
        "total_rows": total_rows,
        "total_skipped": total_skipped
    }


def main():
    parser = argparse.ArgumentParser(
        description="Ingest air quality data to Bronze table",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "--mode",
        choices=["backfill", "upsert"],
        required=True,
        help="Ingestion mode: backfill (historical) or upsert (incremental)"
    )
    
    parser.add_argument(
        "--locations",
        default=os.path.join(ROOT_DIR, "data", "locations.jsonl"),
        help="Path to locations JSONL file (default: data/locations.jsonl)"
    )
    
    parser.add_argument(
        "--start-date",
        help="Start date for backfill (YYYY-MM-DD)"
    )
    
    parser.add_argument(
        "--end-date",
        help="End date for backfill (YYYY-MM-DD, default: today)"
    )
    
    parser.add_argument(
        "--chunk-days",
        type=int,
        default=90,
        help="Days per chunk for backfill (default: 90)"
    )
    
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=7,
        help="Days to look back in upsert mode for gap filling (default: 7)"
    )
    
    parser.add_argument(
        "--override",
        action="store_true",
        help="Override existing data (use with caution!)"
    )
    
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=1.0,
        help="Seconds to wait between API requests (default: 1.0)"
    )
    
    parser.add_argument(
        "--table",
        default="hadoop_catalog.lh.bronze.open_meteo_hourly",
        help="Target table name"
    )
    
    parser.add_argument(
        "--warehouse",
        default=os.getenv("WAREHOUSE_URI", "hdfs://khoa-master:9000/warehouse/iceberg"),
        help="Warehouse URI"
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.mode == "backfill":
        if not args.start_date:
            parser.error("--start-date is required for backfill mode")
        if not args.end_date:
            args.end_date = datetime.now().strftime("%Y-%m-%d")
    
    # Execute ingestion
    result = execute_ingestion(
        mode=args.mode,
        locations_path=args.locations,
        start_date=args.start_date if args.mode == "backfill" else None,
        end_date=args.end_date if args.mode == "backfill" else None,
        chunk_days=args.chunk_days,
        lookback_days=args.lookback_days,
        override=args.override,
        rate_limit=args.rate_limit,
        table=args.table,
        warehouse=args.warehouse
    )
    
    # Exit with appropriate code
    if result["success"]:
        sys.exit(0)
    else:
        sys.exit(1)


def execute_ingestion(
    mode: str,
    locations_path: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    chunk_days: int = 90,
    lookback_days: int = 7,
    override: bool = False,
    rate_limit: float = 1.0,
    table: str = "hadoop_catalog.lh.bronze.open_meteo_hourly",
    warehouse: str = "hdfs://khoa-master:9000/warehouse/iceberg"
) -> Dict:
    """Execute bronze ingestion - Prefect-friendly wrapper.
    
    This function can be called directly from Prefect flows.
    
    Args:
        mode: "backfill" or "upsert"
        locations_path: Path to locations JSONL file
        start_date: Start date for backfill (YYYY-MM-DD)
        end_date: End date for backfill (YYYY-MM-DD)
        chunk_days: Days per chunk for backfill
        lookback_days: Days to look back in upsert mode
        override: Override existing data
        rate_limit: Seconds between API requests
        table: Target table name
        warehouse: Warehouse URI
    
    Returns:
        Dict with:
            - success: bool
            - stats: dict with metrics
            - elapsed_seconds: float
            - error: str (if failed)
    """
    try:
        # Load locations
        print(f"\n📂 Loading locations from {locations_path}...")
        locations = load_locations(locations_path)
        print(f"✓ Loaded {len(locations)} locations")
        
        # Build Spark session (only once!)
        print(f"\n🚀 Starting Spark session...")
        spark = build_spark_session()
        spark.conf.set("spark.sql.catalog.hadoop_catalog.warehouse", warehouse)
        print(f"✓ Spark session ready")
        
        # Run ingestion
        start_time = time.time()
        
        if mode == "backfill":
            if not start_date:
                raise ValueError("start_date is required for backfill mode")
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")
                
            stats = run_backfill(
                spark=spark,
                locations=locations,
                start_date=start_date,
                end_date=end_date,
                table=table,
                chunk_days=chunk_days,
                override=override,
                rate_limit=rate_limit
            )
        else:  # upsert
            stats = run_upsert(
                spark=spark,
                locations=locations,
                table=table,
                lookback_days=lookback_days,
                rate_limit=rate_limit
            )
        
        elapsed = time.time() - start_time
        
        # Summary
        print("\n" + "=" * 70)
        print("✅ INGESTION COMPLETE")
        print("=" * 70)
        print(f"Mode: {mode.upper()}")
        print(f"Locations processed: {len(locations)}")
        print(f"Rows inserted: {stats['total_rows']:,}")
        if 'total_skipped' in stats:
            print(f"Rows skipped: {stats['total_skipped']:,}")
        if 'total_chunks' in stats:
            print(f"Chunks processed: {stats['total_chunks']}")
        print(f"Time elapsed: {elapsed:.1f}s ({elapsed/60:.1f}m)")
        print(f"Target table: {table}")
        print("=" * 70)
        
        # Verify data
        print(f"\n🔍 Verifying data in {table}...")
        try:
            row_count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
            print(f"✓ Total rows in table: {row_count:,}")
            
            # Show sample
            print(f"\n📊 Sample data (latest 5 rows):")
            spark.sql(f"""
                SELECT location_key, ts_utc, date_utc, aqi, pm25, pm10 
                FROM {table} 
                ORDER BY ts_utc DESC 
                LIMIT 5
            """).show(truncate=False)
            
        except Exception as e:
            print(f"⚠ Could not verify: {e}")
        
        spark.stop()
        print("\n✓ Done!")
        
        return {
            "success": True,
            "stats": stats,
            "elapsed_seconds": elapsed
        }
        
    except Exception as e:
        print(f"\n❌ INGESTION FAILED: {e}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "stats": {},
            "elapsed_seconds": 0,
            "error": str(e)
        }


if __name__ == "__main__":
    main()
