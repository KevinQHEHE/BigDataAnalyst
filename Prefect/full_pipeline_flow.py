"""Prefect Full Pipeline Flow: Bronze → Silver → Gold (Optimized).

This flow orchestrates the complete data pipeline with:
- Subprocess jobs with fresh JVM per stage (YARN-compatible)
- Sequential execution: Bronze → Silver → Gold
- NO memory bloat, efficient garbage collection
- Automatic retry and error handling
- Comprehensive logging

"""
import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict

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
    name="Full Pipeline Flow",
    description="Complete Bronze → Silver → Gold pipeline (optimized subprocess approach)",
    log_prints=True
)
def full_pipeline_flow(
    # Bronze parameters
    bronze_mode: str = "upsert",
    bronze_start_date: str = None,
    bronze_end_date: str = None,
    skip_bronze: bool = False,
    
    # Silver parameters
    silver_mode: str = "incremental",
    silver_start_date: str = None,
    silver_end_date: str = None,
    skip_silver: bool = False,
    
    # Gold parameters
    gold_mode: str = "all",
    skip_gold: bool = False,
    aqi_threshold: int = 151,
    min_hours: int = 4,
    
    # Common parameters
    locations_path: str = "hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl",
    pollutants_path: str = "hdfs://khoa-master:9000/user/dlhnhom2/data/dim_pollutant.jsonl",
    warehouse: str = "hdfs://khoa-master:9000/warehouse/iceberg"
) -> Dict:
    """Execute complete Bronze → Silver → Gold pipeline with subprocess jobs.
    
    Each stage runs in a separate JVM process for:
    - Fresh memory/heap (no bloat between stages)
    - Efficient garbage collection
    - Lower resource consumption
    - Better for hourly scheduling
    
    Args:
        bronze_mode: 'backfill' or 'upsert'
        skip_bronze: Skip bronze ingestion
        silver_mode: 'full' or 'incremental'
        skip_silver: Skip silver transformation
        gold_mode: 'all', 'dims', or 'facts'
        skip_gold: Skip gold pipeline
        
    Returns:
        Dictionary with pipeline statistics
    """
    print("="*80)
    print("FULL PIPELINE FLOW: BRONZE → SILVER → GOLD (Optimized)")
    print("="*80)
    
    pipeline_start = time.time()
    results = {"stages": {}}
    
    # ========== BRONZE STAGE ==========
    if not skip_bronze:
        print(f"\n{'='*80}")
        print("STAGE 1: BRONZE INGESTION")
        print(f"{'='*80}")
        
        bronze_args = [
            "--mode", bronze_mode,
            "--locations", locations_path,
            "--override"
        ]
        
        if bronze_start_date:
            bronze_args.extend(["--start-date", bronze_start_date])
        if bronze_end_date:
            bronze_args.extend(["--end-date", bronze_end_date])
        
        success, bronze_result = run_subprocess_job(
            "jobs/bronze/run_bronze_pipeline.py",
            bronze_args,
            "Bronze",
            timeout=1600
        )
        
        results["stages"]["bronze"] = bronze_result
        if not success:
            results["success"] = False
    else:
        print("\nSkipping Bronze stage")
        results["stages"]["bronze"] = "skipped"
    
    # ========== SILVER STAGE ==========
    if not skip_silver:
        print(f"\n{'='*80}")
        print("STAGE 2: SILVER TRANSFORMATION")
        print(f"{'='*80}")
        
        silver_args = ["--mode", silver_mode]
        
        if silver_start_date:
            silver_args.extend(["--start-date", silver_start_date])
        if silver_end_date:
            silver_args.extend(["--end-date", silver_end_date])
        
        success, silver_result = run_subprocess_job(
            "jobs/silver/run_silver_pipeline.py",
            silver_args,
            "Silver",
            timeout=1600
        )
        
        results["stages"]["silver"] = silver_result
        if not success:
            results["success"] = False
    else:
        print("\nSkipping Silver stage")
        results["stages"]["silver"] = "skipped"
    
    # ========== GOLD STAGE ==========
    if not skip_gold:
        print(f"\n{'='*80}")
        print("STAGE 3: GOLD AGGREGATIONS")
        print(f"{'='*80}")
        
        gold_args = [
            "--mode", gold_mode,
            "--locations", locations_path,
            "--pollutants", pollutants_path,
            "--aqi-threshold", str(aqi_threshold),
            "--min-hours", str(min_hours)
        ]
        
        success, gold_result = run_subprocess_job(
            "jobs/gold/run_gold_pipeline.py",
            gold_args,
            "Gold",
            timeout=1800
        )
        
        results["stages"]["gold"] = gold_result
        if not success:
            results["success"] = False
    else:
        print("\nSkipping Gold stage")
        results["stages"]["gold"] = "skipped"
    
    # ========== SUMMARY ==========
    elapsed = time.time() - pipeline_start
    
    print(f"\n{'='*80}")
    print("FULL PIPELINE COMPLETE")
    print(f"{'='*80}")
    
    print("Stage results:")
    for stage, result in results["stages"].items():
        if isinstance(result, dict):
            status = "OK" if result.get("success") else "FAILED"
            elapsed_s = result.get("elapsed_seconds", 0)
            print(f"  [{status}] {stage.upper():10s} {elapsed_s:6.1f}s")
        else:
            print(f"  [SKIP] {stage.upper():10s}")
    
    print(f"\nTotal pipeline time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    print("="*80)
    
    return {
        "success": results.get("success", True),
        "results": results["stages"],
        "elapsed_seconds": elapsed,
        "timestamp": datetime.now().isoformat()
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Full Pipeline Prefect Flow - Submit via spark_submit.sh"
    )
    
    # Bronze options
    bronze_group = parser.add_argument_group("Bronze options")
    bronze_group.add_argument(
        "--bronze-mode",
        choices=["backfill", "upsert"],
        default="upsert"
    )
    bronze_group.add_argument("--bronze-start-date", help="Bronze start date (YYYY-MM-DD)")
    bronze_group.add_argument("--bronze-end-date", help="Bronze end date (YYYY-MM-DD)")
    bronze_group.add_argument("--skip-bronze", action="store_true")
    
    # Silver options
    silver_group = parser.add_argument_group("Silver options")
    silver_group.add_argument(
        "--silver-mode",
        choices=["full", "incremental"],
        default="incremental"
    )
    silver_group.add_argument("--start-date", dest="silver_start_date", help="Silver start date (YYYY-MM-DD)")
    silver_group.add_argument("--end-date", dest="silver_end_date", help="Silver end date (YYYY-MM-DD)")
    silver_group.add_argument("--skip-silver", action="store_true")
    
    # Gold options
    gold_group = parser.add_argument_group("Gold options")
    gold_group.add_argument(
        "--gold-mode",
        choices=["all", "dims", "facts"],
        default="all"
    )
    gold_group.add_argument("--skip-gold", action="store_true")
    gold_group.add_argument("--aqi-threshold", type=int, default=151)
    gold_group.add_argument("--min-hours", type=int, default=4)
    
    # Common options
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
        "--hourly",
        action="store_true",
        help="Flag to indicate hourly execution mode"
    )
    
    args = parser.parse_args()
    
    # Run full pipeline
    result = full_pipeline_flow(
        bronze_mode=args.bronze_mode,
        bronze_start_date=args.bronze_start_date,
        bronze_end_date=args.bronze_end_date,
        skip_bronze=args.skip_bronze,
        silver_mode=args.silver_mode,
        silver_start_date=args.silver_start_date,
        silver_end_date=args.silver_end_date,
        skip_silver=args.skip_silver,
        gold_mode=args.gold_mode,
        skip_gold=args.skip_gold,
        aqi_threshold=args.aqi_threshold,
        min_hours=args.min_hours,
        locations_path=args.locations,
        pollutants_path=args.pollutants,
        warehouse=args.warehouse
    )
    
    sys.exit(0 if result["success"] else 1)
