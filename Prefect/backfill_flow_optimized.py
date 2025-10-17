"""Prefect Backfill Flow: Optimized Historical Data Processing.

This flow processes large date ranges by:
- Spawning subprocess jobs with fresh JVM per stage
- NO SparkSession reuse, NO Prefect overhead per chunk
- Tracking progress and collecting metrics
- Generating comprehensive summary report

KEY DIFFERENCE from old backfill_flow.py:
  OLD: bronze_flow -> silver_flow -> gold_flow (3 Prefect flows, 1 SparkSession)
       - SparkSession reused across chunks
       - Memory bloat from long-lived session
       - Prefect overhead on each task
       - Slow GC

  NEW: spark-submit job1 -> spark-submit job2 -> spark-submit job3
       - Fresh JVM for each job stage
       - Clean memory + efficient GC
       - No Prefect overhead per chunk
       - 3-5x faster

Usage (via YARN):
  bash scripts/spark_submit.sh Prefect/backfill_flow_optimized.py -- \\
    --start-date 2024-01-01 \\
    --end-date 2024-12-31

Run via YARN:
  Must submit via scripts/spark_submit.sh wrapper for YARN deployment
  Direct python execution is not supported
"""
import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Dict, List, Tuple

from prefect import flow

# Setup paths
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"

for path in [SRC_DIR]:
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from dotenv import load_dotenv
load_dotenv(ROOT_DIR / ".env")


def run_subprocess_job(
    script_path: str,
    args: List[str],
    job_name: str = "job",
    timeout: int = 3600
) -> Tuple[bool, Dict]:
    """Run a Spark job as subprocess with fresh JVM.
    
    Args:
        script_path: Relative path to script (e.g., "jobs/bronze/run_bronze_pipeline.py")
        args: Command-line arguments
        job_name: Name for logging
        timeout: Timeout in seconds
        
    Returns:
        (success: bool, result: dict)
    """
    start_time = time.time()
    script_full = ROOT_DIR / script_path
    
    if not script_full.exists():
        return False, {"error": f"Script not found: {script_full}"}
    
    cmd = [
        "bash",
        str(ROOT_DIR / "scripts/spark_submit.sh"),
        str(script_path),
        "--"
    ] + args
    
    print(f"\n[{job_name}] Starting subprocess (fresh JVM)...")
    print(f"[{job_name}] Command: {' '.join(cmd[-4:])}")
    print(f"[{job_name}] Output:")
    print("-" * 80)
    sys.stdout.flush()
    
    try:
        # Use Popen for real-time output streaming (prevents Prefect lag)
        process = subprocess.Popen(
            cmd,
            cwd=str(ROOT_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1  # Line buffered for real-time output
        )
        
        # Stream output line by line
        for line in process.stdout:
            print(line, end='', flush=True)  # Explicit flush for Prefect logging
        
        # Wait for process to complete
        return_code = process.wait(timeout=timeout)
        elapsed = time.time() - start_time
        print("-" * 80)
        sys.stdout.flush()
        
        if return_code == 0:
            print(f"[{job_name}] SUCCESS ({elapsed:.1f}s)")
            return True, {
                "success": True,
                "elapsed_seconds": elapsed
            }
        else:
            print(f"[{job_name}] FAILED (exit {return_code})")
            return False, {
                "success": False,
                "exit_code": return_code,
                "elapsed_seconds": elapsed
            }
    
    except subprocess.TimeoutExpired:
        print("-" * 80)
        print(f"[{job_name}] TIMEOUT ({timeout}s)")
        return False, {
            "success": False,
            "error": f"Timeout after {timeout}s",
            "elapsed_seconds": timeout
        }
    except Exception as e:
        print("-" * 80)
        print(f"[{job_name}] EXCEPTION: {e}")
        return False, {
            "success": False,
            "error": str(e),
            "elapsed_seconds": time.time() - start_time
        }


@flow(
    name="Backfill Flow Optimized",
    description="Optimized backfill with subprocess jobs (fresh JVM per stage)",
    log_prints=True
)
def backfill_flow(
    start_date: str,
    end_date: str,
    skip_bronze: bool = False,
    skip_silver: bool = False,
    skip_gold: bool = False,
    locations_path: str = "hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl",
    pollutants_path: str = "hdfs://khoa-master:9000/user/dlhnhom2/data/dim_pollutant.jsonl",
    warehouse: str = "hdfs://khoa-master:9000/warehouse/iceberg"
) -> Dict:
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
    print(f"Warehouse: {warehouse}")
    
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
            timeout=7200  # 2 hours
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
            timeout=7200
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
            timeout=3600
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
