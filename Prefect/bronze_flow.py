"""Prefect Flow for Bronze Layer Ingestion.

This flow orchestrates the Bronze layer ingestion from Open-Meteo API with:
- Single SparkSession per flow execution (YARN-compatible)
- Automatic retry on failure
- Support for backfill and upsert modes
- Proper logging and metrics

Usage (via YARN):
  bash scripts/spark_submit.sh Prefect/bronze_flow.py -- --mode upsert
  bash scripts/spark_submit.sh Prefect/bronze_flow.py -- --mode backfill --start-date 2024-01-01 --end-date 2024-12-31

DO NOT run directly with python - use spark_submit.sh wrapper for YARN deployment.
"""
import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from prefect import flow, task

# Setup paths
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"

for path in [SRC_DIR]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from dotenv import load_dotenv
load_dotenv(ROOT_DIR / ".env")

# Import Spark context manager
from spark_context import get_spark_session, log_spark_info, validate_yarn_mode


@task(
    name="Load Location Configuration",
    description="Load location list from HDFS or local file",
    retries=2,
    retry_delay_seconds=10,
    log_prints=True
)
def load_locations_task(locations_path: str, spark_app_id: str) -> List[Dict]:
    """Load locations configuration.
    
    Args:
        locations_path: Path to locations file (JSONL or JSON)
        spark_app_id: Current Spark application ID for logging
        
    Returns:
        List of location dictionaries
    """
    from bronze.run_bronze_pipeline import load_locations
    from pyspark.sql import SparkSession
    
    print(f"Loading locations from: {locations_path}")
    print(f"Using Spark application: {spark_app_id}")
    
    # Get active spark session by app_id
    spark = SparkSession.builder.getOrCreate()
    
    locations = load_locations(locations_path, spark=spark)
    print(f"✓ Loaded {len(locations)} locations")
    
    return locations


@task(
    name="Ingest Location Chunk",
    description="Ingest data for one location and date range",
    retries=2,
    retry_delay_seconds=30,
    log_prints=True
)
def ingest_location_chunk_task(
    location: Dict,
    start_date: str,
    end_date: str,
    table: str,
    override: bool
) -> Dict:
    """Ingest data for a single location and date range.
    
    Args:
        location: Location dictionary
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        table: Target Iceberg table
        override: Whether to override existing data
        
    Returns:
        Dictionary with ingestion statistics
    """
    from bronze.run_bronze_pipeline import ingest_location_chunk
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    loc_key = location.get("location_key") or location.get("id") or location.get("name")
    loc_name = location.get("location_name") or location.get("name", "Unknown")
    
    print(f"Processing: {loc_name} ({loc_key})")
    print(f"  Date range: {start_date} to {end_date}")
    print(f"  Override: {override}")
    
    rows = ingest_location_chunk(
        spark=spark,
        location=location,
        start=start_date,
        end=end_date,
        table=table,
        override=override
    )
    
    return {
        "location_key": loc_key,
        "location_name": loc_name,
        "rows_ingested": rows,
        "start_date": start_date,
        "end_date": end_date
    }


@flow(
    name="Bronze Ingestion Flow",
    description="Ingest air quality data from Open-Meteo API to Bronze layer",
    log_prints=True
)
def bronze_ingestion_flow(
    mode: str = "upsert",
    locations_path: str = "hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    chunk_days: int = 90,
    override: bool = False,
    table: str = "hadoop_catalog.lh.bronze.open_meteo_hourly",
    warehouse: str = "hdfs://khoa-master:9000/warehouse/iceberg",
    require_yarn: bool = True
) -> Dict:
    """Bronze ingestion flow with single SparkSession.
    
    Args:
        mode: 'backfill' or 'upsert'
        locations_path: Path to locations configuration
        start_date: Start date for backfill (YYYY-MM-DD)
        end_date: End date for backfill (YYYY-MM-DD)
        chunk_days: Days per API request chunk
        override: Override existing data
        table: Target Iceberg table
        warehouse: Iceberg warehouse URI
        require_yarn: Validate YARN deployment
        
    Returns:
        Dictionary with flow statistics
    """
    from bronze.run_bronze_pipeline import generate_date_chunks
    
    print("="*80)
    print("BRONZE INGESTION FLOW")
    print("="*80)
    print(f"Mode: {mode}")
    print(f"Locations: {locations_path}")
    print(f"Table: {table}")
    print(f"Warehouse: {warehouse}")
    
    if mode == "backfill":
        if not start_date:
            raise ValueError("start_date required for backfill mode")
        print(f"Date range: {start_date} to {end_date or 'today'}")
    
    flow_start = time.time()
    
    # Create single Spark session for entire flow
    with get_spark_session(
        app_name="bronze_ingestion_flow",
        require_yarn=require_yarn
    ) as spark:
        
        # Validate YARN if required
        if require_yarn:
            validate_yarn_mode(spark)
        
        # Log Spark info
        log_spark_info(spark, "Bronze Flow")
        
        # Set warehouse
        spark.conf.set("spark.sql.catalog.hadoop_catalog.warehouse", warehouse)
        
        # Get Spark application ID for tasks
        app_id = spark.sparkContext.applicationId
        
        # Load locations
        locations = load_locations_task(locations_path, app_id)
        
        print(f"\n{'='*60}")
        print(f"Processing {len(locations)} locations")
        print(f"{'='*60}\n")
        
        results = []
        total_rows = 0
        
        if mode == "backfill":
            # Backfill mode: process all locations for date range
            end = end_date or datetime.now().strftime("%Y-%m-%d")
            chunks = generate_date_chunks(start_date, end, chunk_days)
            
            print(f"Generated {len(chunks)} date chunks")
            for chunk_start, chunk_end in chunks:
                print(f"  {chunk_start} to {chunk_end}")
            print()
            
            for location in locations:
                for chunk_start, chunk_end in chunks:
                    result = ingest_location_chunk_task(
                        location=location,
                        start_date=chunk_start,
                        end_date=chunk_end,
                        table=table,
                        override=override
                    )
                    results.append(result)
                    total_rows += result["rows_ingested"]
                    
                    # Small delay to avoid API rate limiting
                    time.sleep(1)
        
        else:  # upsert mode
            # Upsert mode: update from latest timestamp to now (hourly incremental)
            from bronze.run_bronze_pipeline import get_latest_timestamp
            
            now = datetime.now()
            current_date = now.strftime("%Y-%m-%d")
            locations_updated = 0
            
            for location in locations:
                loc_key = location.get("location_key") or location.get("id") or location.get("name")
                loc_name = location.get("location_name") or location.get("name", "Unknown")
                
                # Get latest timestamp
                latest = get_latest_timestamp(spark, table, loc_key)
                if not latest:
                    print(f"No existing data, skipping upsert: {loc_name}")
                    continue
                
                latest_str = latest.strftime("%Y-%m-%d %H:%M:%S")
                latest_ts = datetime.strptime(latest_str, "%Y-%m-%d %H:%M:%S")
                print(f"↻ {loc_name}: Latest {latest_str}")
                
                # Check if we're already up to date
                # Use absolute value to handle timezone issues
                hours_diff = abs((now - latest_ts).total_seconds() / 3600)
                
                if hours_diff < 1:  # Less than 1 hours difference
                    print(f"  ✓ Already up to date ({hours_diff:.1f}h from now)")
                    continue
                
                # Calculate date range to ingest
                # For incremental hourly mode, only fetch current date to minimize API calls
                # The filtering logic in run_bronze_pipeline will filter out existing timestamps
                latest_date = latest_ts.strftime("%Y-%m-%d")
                
                # Only ingest current date (today) for true hourly incremental behavior
                # This reduces unnecessary API calls for yesterday's data which already exists
                result = ingest_location_chunk_task(
                    location=location,
                    start_date=current_date,  # Only fetch today
                    end_date=current_date,
                    table=table,
                    override=False
                )
                
                if result["rows_ingested"] > 0:
                    locations_updated += 1
                    results.append(result)
                    total_rows += result["rows_ingested"]
                
                time.sleep(1)
            
            print(f"\n✓ Updated {locations_updated}/{len(locations)} locations")
        
        # Final statistics
        elapsed = time.time() - flow_start
        
        print(f"\n{'='*80}")
        print("BRONZE FLOW COMPLETE")
        print(f"{'='*80}")
        print(f"Total rows ingested: {total_rows}")
        print(f"Locations processed: {len(results)}")
        print(f"Elapsed time: {elapsed:.1f}s")
        
        # Verify table count
        try:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
            print(f"Total rows in table: {count:,}")
        except Exception as e:
            print(f"Warning: Could not verify table count: {e}")
        
        return {
            "success": True,
            "mode": mode,
            "total_rows": total_rows,
            "locations_count": len(locations),
            "chunks_processed": len(results),
            "elapsed_seconds": elapsed,
            "results": results
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Bronze Ingestion Prefect Flow - Submit via spark_submit.sh"
    )
    parser.add_argument("--mode", choices=["backfill", "upsert"], default="upsert")
    parser.add_argument(
        "--locations",
        default="hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl"
    )
    parser.add_argument("--start-date", help="Start date for backfill (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date for backfill (YYYY-MM-DD)")
    parser.add_argument("--chunk-days", type=int, default=90)
    parser.add_argument("--override", action="store_true")
    parser.add_argument("--table", default="hadoop_catalog.lh.bronze.open_meteo_hourly")
    parser.add_argument(
        "--warehouse",
        default=os.getenv("WAREHOUSE_URI", "hdfs://khoa-master:9000/warehouse/iceberg")
    )
    parser.add_argument(
        "--no-yarn-check",
        action="store_true",
        help="Skip YARN validation (for local testing only)"
    )
    
    args = parser.parse_args()
    
    if args.mode == "backfill" and not args.start_date:
        parser.error("--start-date required for backfill mode")
    
    # Run flow
    result = bronze_ingestion_flow(
        mode=args.mode,
        locations_path=args.locations,
        start_date=args.start_date,
        end_date=args.end_date,
        chunk_days=args.chunk_days,
        override=args.override,
        table=args.table,
        warehouse=args.warehouse,
        require_yarn=not args.no_yarn_check
    )
    
    sys.exit(0 if result["success"] else 1)
