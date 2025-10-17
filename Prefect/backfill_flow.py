"""Prefect Backfill Flow: Optimized Historical Data Processing.

This flow processes large date ranges with:
- Subprocess jobs with fresh JVM per stage
- Each stage runs in a separate process with clean memory
- Tracking progress and collecting metrics
- Generating comprehensive summary report

Usage (via YARN):
  bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
    --start-date 2024-01-01 \\
    --end-date 2024-12-31

Run via YARN:
  Must submit via scripts/spark_submit.sh wrapper for YARN deployment
  Direct python execution is not supported
"""
import argparse
import os
import sys
import time
from pathlib import Path

from prefect import flow

from utils import run_subprocess_job

# Setup paths
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"

for path in [SRC_DIR]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
from dotenv import load_dotenv
load_dotenv(ROOT_DIR / ".env")


@flow(
    name="Backfill Flow",
    description="Backfill with subprocess jobs",
    log_prints=True
)
def backfill_flow(
    start_date: str,
    end_date: str,
    skip_bronze: bool = False,
    skip_silver: bool = False,
    skip_gold: bool = False,
    locations_path: str = "hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl",
    pollutants_path: str = "hdfs://khoa-master:9000/user/dlhnhom2/data/dim_pollutant.jsonl"
) -> dict:
    """Execute optimized backfill with subprocess jobs.
    
    This replaces the old backfill_flow.py with a simpler approach:
    - Bronze job (spark-submit) → process backfill
    - Silver job (spark-submit) → full refresh
    - Gold job (spark-submit) → all aggregations
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        skip_bronze: Skip bronze stage
        skip_silver: Skip silver stage
        skip_gold: Skip gold stage
        locations_path: Path to locations file
        pollutants_path: Path to pollutants file
        warehouse: Iceberg warehouse URI
        
    Returns:
        Dictionary with results for each stage
    """
    print("\n" + "="*80)
    print("BACKFILL FLOW - OPTIMIZED (subprocess jobs)")
    print("="*80)
    print(f"Date range: {start_date} to {end_date}")
    
    stages = []
    if not skip_bronze:
        stages.append("Bronze")
    if not skip_silver:
        stages.append("Silver")
    if not skip_gold:
        stages.append("Gold")
    
    print(f"Pipeline: {' - '.join(stages)}")
    print("="*80)
    
    backfill_start = time.time()
    results = {
        "success": True,
        "start_date": start_date,
        "end_date": end_date,
        "stages": {},
        "elapsed_seconds": 0,
        "errors": []
    }
    
    # ========== BRONZE STAGE ==========
    if not skip_bronze:
        print(f"\n{'='*80}")
        print("STAGE 1: BRONZE INGESTION")
        print(f"{'='*80}")
        
        bronze_args = [
            "--mode", "backfill",
            "--start-date", start_date,
            "--end-date", end_date,
            "--locations", locations_path,
            "--override"
        ]
        
        success, bronze_result = run_subprocess_job(
            "jobs/bronze/run_bronze_pipeline.py",
            bronze_args,
            "Bronze",
            timeout=1200  # 20 minutes
        )
        
        results["stages"]["bronze"] = bronze_result
        if not success:
            results["success"] = False
            results["errors"].append(f"Bronze: {bronze_result.get('error')}")
            print("\nWARNING: Continuing with Silver/Gold despite Bronze failure...")
    
    # ========== SILVER STAGE ==========
    if not skip_silver:
        print(f"\n{'='*80}")
        print("STAGE 2: SILVER TRANSFORMATION")
        print(f"{'='*80}")
        
        silver_args = [
            "--mode", "full"
        ]
        
        success, silver_result = run_subprocess_job(
            "jobs/silver/run_silver_pipeline.py",
            silver_args,
            "Silver",
            timeout=1200
        )
        
        results["stages"]["silver"] = silver_result
        if not success:
            results["success"] = False
            results["errors"].append(f"Silver: {silver_result.get('error')}")
            print("\nWARNING: Continuing with Gold despite Silver failure...")
    
    # ========== GOLD STAGE ==========
    if not skip_gold:
        print(f"\n{'='*80}")
        print("STAGE 3: GOLD AGGREGATIONS")
        print(f"{'='*80}")
        
        gold_args = [
            "--mode", "all",
            "--locations", locations_path,
            "--pollutants", pollutants_path
        ]
        
        success, gold_result = run_subprocess_job(
            "jobs/gold/run_gold_pipeline.py",
            gold_args,
            "Gold",
            timeout=1600
        )
        
        results["stages"]["gold"] = gold_result
        if not success:
            results["success"] = False
            results["errors"].append(f"Gold: {gold_result.get('error')}")
    
    # ========== SUMMARY ==========
    elapsed = time.time() - backfill_start
    results["elapsed_seconds"] = elapsed
    
    print(f"\n{'='*80}")
    print("BACKFILL COMPLETE")
    print(f"{'='*80}")
    print(f"Status: SUCCESS" if results["success"] else "Status: PARTIAL/FAILED")
    print(f"Total time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    
    print("\nStage results:")
    for stage, result in results["stages"].items():
        status = "OK" if result.get("success") else "FAILED"
        elapsed_s = result.get("elapsed_seconds", 0)
        print(f"  [{status}] {stage.upper():10s} {elapsed_s:6.1f}s")
    
    if results["errors"]:
        print("\nErrors:")
        for error in results["errors"]:
            print(f"  - {error}")
    
    print("="*80 + "\n")
    
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Optimized Backfill Flow - Must run via scripts/spark_submit.sh"
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
        "--skip-bronze",
        action="store_true",
        help="Skip bronze stage"
    )
    parser.add_argument(
        "--skip-silver",
        action="store_true",
        help="Skip silver stage"
    )
    parser.add_argument(
        "--skip-gold",
        action="store_true",
        help="Skip gold stage"
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
    
    args = parser.parse_args()
    
    result = backfill_flow(
        start_date=args.start_date,
        end_date=args.end_date,
        skip_bronze=args.skip_bronze,
        skip_silver=args.skip_silver,
        skip_gold=args.skip_gold,
        locations_path=args.locations,
        pollutants_path=args.pollutants,
        warehouse=args.warehouse
    )
    
    sys.exit(0 if result["success"] else 1)
