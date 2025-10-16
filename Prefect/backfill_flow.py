"""Prefect Backfill Flow: Historical data processing with chunking.

This flow processes large date ranges by:
- Chunking date range into monthly or weekly segments
- Running full pipeline for each chunk
- Tracking progress and collecting metrics
- Generating comprehensive summary report

Usage (via YARN):
  # Backfill entire year by month
  bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
    --start-date 2024-01-01 \\
    --end-date 2024-12-31 \\
    --chunk-mode monthly

  # Backfill quarter by week
  bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
    --start-date 2024-10-01 \\
    --end-date 2024-12-31 \\
    --chunk-mode weekly

DO NOT run directly with python - use spark_submit.sh wrapper for YARN deployment.
"""
import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

from prefect import flow

# Setup paths
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
PREFECT_DIR = ROOT_DIR / "Prefect"

for path in [SRC_DIR, str(PREFECT_DIR)]:
    if path not in sys.path:
        sys.path.insert(0, path)

from dotenv import load_dotenv
load_dotenv(ROOT_DIR / ".env")

# Import Spark context manager
from spark_context import get_spark_session, log_spark_info, validate_yarn_mode

# Import flows
from bronze_flow import bronze_ingestion_flow
from silver_flow import silver_transformation_flow
from gold_flow import gold_pipeline_flow


def generate_date_chunks(
    start_date: str,
    end_date: str,
    chunk_mode: str = "monthly"
) -> List[Tuple[str, str]]:
    """Generate date chunks for backfill processing.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        chunk_mode: 'monthly', 'weekly', or 'daily'
        
    Returns:
        List of (chunk_start, chunk_end) tuples
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    chunks = []
    current = start
    
    while current <= end:
        if chunk_mode == "monthly":
            # Monthly chunks: start of month to end of month
            chunk_start = current.replace(day=1)
            # Last day of month
            if current.month == 12:
                chunk_end = current.replace(day=31)
            else:
                next_month = current.replace(month=current.month + 1, day=1)
                chunk_end = next_month - timedelta(days=1)
            
            # Don't exceed overall end date
            if chunk_end > end:
                chunk_end = end
            
            chunks.append((
                chunk_start.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d")
            ))
            
            # Move to next month
            if current.month == 12:
                current = current.replace(year=current.year + 1, month=1, day=1)
            else:
                current = current.replace(month=current.month + 1, day=1)
        
        elif chunk_mode == "weekly":
            # Weekly chunks: 7 days at a time
            chunk_start = current
            chunk_end = min(current + timedelta(days=6), end)
            
            chunks.append((
                chunk_start.strftime("%Y-%m-%d"),
                chunk_end.strftime("%Y-%m-%d")
            ))
            
            current = chunk_end + timedelta(days=1)
        
        elif chunk_mode == "daily":
            # Daily chunks: one day at a time
            chunks.append((
                current.strftime("%Y-%m-%d"),
                current.strftime("%Y-%m-%d")
            ))
            
            current += timedelta(days=1)
        
        else:
            raise ValueError(f"Invalid chunk_mode: {chunk_mode}")
        
        # Safety check to prevent infinite loop
        if len(chunks) > 10000:
            raise ValueError("Too many chunks generated (>10000). Check date range.")
    
    return chunks


@flow(
    name="Backfill Flow",
    description="Historical data backfill with date range chunking",
    log_prints=True
)
def backfill_flow(
    start_date: str,
    end_date: str,
    chunk_mode: str = "monthly",
    include_bronze: bool = True,
    include_silver: bool = True,
    include_gold: bool = True,
    locations_path: str = "hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl",
    pollutants_path: str = "hdfs://khoa-master:9000/user/dlhnhom2/data/dim_pollutant.jsonl",
    warehouse: str = "hdfs://khoa-master:9000/warehouse/iceberg",
    require_yarn: bool = True
) -> Dict:
    """Execute backfill for historical date range with chunking.
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        chunk_mode: 'monthly', 'weekly', or 'daily'
        include_bronze: Include bronze ingestion
        include_silver: Include silver transformation
        include_gold: Include gold pipeline
        locations_path: Path to locations configuration
        pollutants_path: Path to pollutants configuration
        warehouse: Iceberg warehouse URI
        require_yarn: Validate YARN deployment
        
    Returns:
        Dictionary with backfill statistics
    """
    print("="*80)
    print("BACKFILL FLOW: HISTORICAL DATA PROCESSING")
    print("="*80)
    print(f"Date range: {start_date} to {end_date}")
    print(f"Chunk mode: {chunk_mode}")
    print(f"Stages: ", end="")
    stages = []
    if include_bronze:
        stages.append("Bronze")
    if include_silver:
        stages.append("Silver")
    if include_gold:
        stages.append("Gold")
    print(" → ".join(stages))
    print("="*80)
    
    backfill_start = time.time()
    
    # Generate chunks
    chunks = generate_date_chunks(start_date, end_date, chunk_mode)
    
    print(f"\nGenerated {len(chunks)} chunks:")
    for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
        print(f"  Chunk {i:3d}: {chunk_start} to {chunk_end}")
    print()
    
    # Track results
    chunk_results = []
    total_bronze_rows = 0
    total_silver_rows = 0
    total_gold_records = 0
    successful_chunks = 0
    failed_chunks = []
    
    # Create single Spark session for entire backfill
    with get_spark_session(
        app_name="backfill_flow",
        require_yarn=require_yarn
    ) as spark:
        
        # Validate YARN if required
        if require_yarn:
            validate_yarn_mode(spark)
        
        # Log Spark info
        log_spark_info(spark, "Backfill Flow")
        
        # Set warehouse
        spark.conf.set("spark.sql.catalog.hadoop_catalog.warehouse", warehouse)
        
        # Process each chunk
        for i, (chunk_start, chunk_end) in enumerate(chunks, 1):
            chunk_start_time = time.time()
            
            print(f"\n{'='*80}")
            print(f"PROCESSING CHUNK {i}/{len(chunks)}: {chunk_start} to {chunk_end}")
            print(f"{'='*80}")
            
            chunk_result = {
                "chunk_number": i,
                "start_date": chunk_start,
                "end_date": chunk_end,
                "success": False
            }
            
            try:
                # Bronze ingestion
                if include_bronze:
                    print(f"\n[Chunk {i}] Bronze ingestion...")
                    bronze_result = bronze_ingestion_flow(
                        mode="backfill",
                        locations_path=locations_path,
                        start_date=chunk_start,
                        end_date=chunk_end,
                        override=True,  # Override existing data in backfill
                        table="hadoop_catalog.lh.bronze.open_meteo_hourly",
                        warehouse=warehouse,
                        require_yarn=False  # Already validated
                    )
                    chunk_result["bronze"] = bronze_result
                    total_bronze_rows += bronze_result.get("total_rows", 0)
                    print(f"[Chunk {i}] ✓ Bronze: {bronze_result.get('total_rows', 0)} rows")
                
                # Silver transformation
                if include_silver:
                    print(f"\n[Chunk {i}] Silver transformation...")
                    silver_result = silver_transformation_flow(
                        mode="incremental",  # Use merge mode for backfill
                        start_date=chunk_start,
                        end_date=chunk_end,
                        skip_validation=True,  # Skip validation for speed
                        bronze_table="hadoop_catalog.lh.bronze.open_meteo_hourly",
                        silver_table="hadoop_catalog.lh.silver.air_quality_hourly_clean",
                        warehouse=warehouse,
                        require_yarn=False
                    )
                    chunk_result["silver"] = silver_result
                    records = silver_result.get("transformation", {}).get("records_processed", 0)
                    total_silver_rows += records
                    print(f"[Chunk {i}] ✓ Silver: {records} rows")
                
                # Mark chunk as successful
                chunk_result["success"] = True
                successful_chunks += 1
                
                chunk_elapsed = time.time() - chunk_start_time
                chunk_result["elapsed_seconds"] = chunk_elapsed
                print(f"\n[Chunk {i}] ✓ Complete in {chunk_elapsed:.1f}s")
                
            except Exception as e:
                chunk_result["success"] = False
                chunk_result["error"] = str(e)
                failed_chunks.append(i)
                print(f"\n[Chunk {i}] ✗ Failed: {e}")
            
            chunk_results.append(chunk_result)
        
        # Gold pipeline (run once after all chunks)
        if include_gold and successful_chunks > 0:
            print(f"\n{'='*80}")
            print(f"GOLD PIPELINE (after all chunks)")
            print(f"{'='*80}\n")
            
            try:
                gold_result = gold_pipeline_flow(
                    mode="all",
                    locations_path=locations_path,
                    pollutants_path=pollutants_path,
                    warehouse=warehouse,
                    aqi_threshold=151,
                    min_hours=4,
                    require_yarn=False
                )
                total_gold_records = gold_result.get("total_records", 0)
                print(f"✓ Gold complete: {total_gold_records} records")
            except Exception as e:
                print(f"✗ Gold failed: {e}")
    
    # === BACKFILL SUMMARY ===
    elapsed = time.time() - backfill_start
    
    print(f"\n{'='*80}")
    print("BACKFILL COMPLETE")
    print(f"{'='*80}")
    print(f"Date range: {start_date} to {end_date}")
    print(f"Chunk mode: {chunk_mode}")
    print(f"Total chunks: {len(chunks)}")
    print(f"Successful: {successful_chunks}")
    print(f"Failed: {len(failed_chunks)}")
    
    if failed_chunks:
        print(f"\nFailed chunks: {', '.join(map(str, failed_chunks))}")
    
    print(f"\nData processed:")
    if include_bronze:
        print(f"  Bronze rows: {total_bronze_rows:,}")
    if include_silver:
        print(f"  Silver rows: {total_silver_rows:,}")
    if include_gold:
        print(f"  Gold records: {total_gold_records:,}")
    
    print(f"\nTotal time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    print(f"Average per chunk: {elapsed/len(chunks):.1f}s")
    print("="*80)
    
    return {
        "success": len(failed_chunks) == 0,
        "start_date": start_date,
        "end_date": end_date,
        "chunk_mode": chunk_mode,
        "total_chunks": len(chunks),
        "successful_chunks": successful_chunks,
        "failed_chunks": failed_chunks,
        "total_bronze_rows": total_bronze_rows,
        "total_silver_rows": total_silver_rows,
        "total_gold_records": total_gold_records,
        "elapsed_seconds": elapsed,
        "chunk_results": chunk_results
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill Flow with Chunking - Submit via spark_submit.sh"
    )
    
    parser.add_argument(
        "--start-date",
        required=True,
        help="Start date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end-date",
        required=True,
        help="End date (YYYY-MM-DD)"
    )
    parser.add_argument(
        "--chunk-mode",
        choices=["monthly", "weekly", "daily"],
        default="monthly",
        help="Chunking strategy"
    )
    parser.add_argument(
        "--skip-bronze",
        action="store_true",
        help="Skip bronze ingestion"
    )
    parser.add_argument(
        "--skip-silver",
        action="store_true",
        help="Skip silver transformation"
    )
    parser.add_argument(
        "--skip-gold",
        action="store_true",
        help="Skip gold pipeline"
    )
    parser.add_argument(
        "--locations",
        default="hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl"
    )
    parser.add_argument(
        "--pollutants",
        default="hdfs://khoa-master:9000/user/dlhnhom2/data/dim_pollutant.jsonl"
    )
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
    
    # Run backfill
    result = backfill_flow(
        start_date=args.start_date,
        end_date=args.end_date,
        chunk_mode=args.chunk_mode,
        include_bronze=not args.skip_bronze,
        include_silver=not args.skip_silver,
        include_gold=not args.skip_gold,
        locations_path=args.locations,
        pollutants_path=args.pollutants,
        warehouse=args.warehouse,
        require_yarn=not args.no_yarn_check
    )
    
    sys.exit(0 if result["success"] else 1)
