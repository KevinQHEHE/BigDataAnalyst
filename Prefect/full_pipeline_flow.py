"""Prefect Full Pipeline Flow: Bronze → Silver → Gold (Optimized).

This flow orchestrates the complete data pipeline with:
- Bronze: Uses run_bronze_pipeline.py (backfill or upsert)
- Silver: Uses run_silver_pipeline.py (full or incremental with auto-detect)
- Gold Dimensions: dim_location, dim_pollutant, dim_time (pre-loaded, only refresh when needed)
- Gold Facts: fact_hourly, fact_daily, fact_episode (incremental or backfill)
- Subprocess jobs with fresh JVM per stage (YARN-compatible)
- Sequential execution with proper data flow
- Automatic retry and error handling

Usage:
  # Backfill mode: Load historical data for specific date range
  python Prefect/full_pipeline_flow.py --mode backfill --start-date 2024-10-01 --end-date 2024-10-31
  
  # Incremental mode: Update from latest to now (for hourly scheduling)
  python Prefect/full_pipeline_flow.py --mode incremental
  
  # Load dimensions only (when needed)
  python Prefect/full_pipeline_flow.py --mode dims-only

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
    # Pipeline mode: 'backfill', 'incremental', or 'dims-only'
    mode: str = "incremental",
    
    # Date range (for backfill mode)
    start_date: str = None,
    end_date: str = None,
    
    # Control flags
    skip_bronze: bool = False,
    skip_silver: bool = False,
    skip_gold: bool = False,
    load_dims: bool = False,  # Only load dimensions in gold
    
    # Gold parameters
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
    
    Pipeline Modes:
        backfill: Load historical data for specific date range
                 Bronze: backfill mode with date range
                 Silver: full mode with date range
                 Gold: Load dims (if load_dims=True), then facts with full mode
        
        incremental: Update from latest data to now (for hourly scheduling)
                    Bronze: upsert mode (auto-detect latest and update)
                    Silver: incremental mode with auto-detect
                    Gold: Facts only with incremental mode (auto-detect)
        
        dims-only: Load/refresh gold dimensions only (location, pollutant, time, date)
    
    Args:
        mode: Pipeline mode - 'backfill', 'incremental', or 'dims-only'
        start_date: Start date (YYYY-MM-DD) for backfill mode
        end_date: End date (YYYY-MM-DD) for backfill mode
        skip_bronze: Skip bronze stage
        skip_silver: Skip silver stage
        skip_gold: Skip gold stage
        load_dims: Load gold dimensions (auto-enabled for backfill and dims-only)
        
    Returns:
        Dictionary with pipeline statistics
    """
    print("="*80)
    print(f"FULL PIPELINE FLOW: {mode.upper()} MODE")
    print("="*80)
    print(f"Date range: {start_date or 'auto'} to {end_date or 'now'}")
    print(f"Load dimensions: {load_dims or mode in ['backfill', 'dims-only']}")
    print("="*80)
    
    pipeline_start = time.time()
    results = {"stages": {}, "mode": mode}
    
    # Auto-enable dims for backfill and dims-only mode
    if mode in ["backfill", "dims-only"]:
        load_dims = True
    
        # Determine bronze mode based on pipeline mode
    if mode == "backfill":
        bronze_mode = "backfill"
        silver_mode = "full"
        gold_load_mode = "full"
        load_dims = True  # Always load dims in backfill mode
    elif mode == "incremental":
        bronze_mode = "incremental"  # incremental from latest
        silver_mode = "incremental"
        gold_load_mode = "incremental"
        load_dims = False  # Skip dims in incremental mode (already loaded)
    elif mode == "dims-only":
        skip_bronze = True
        skip_silver = True
        skip_gold = False
        load_dims = True
        # Gold mode doesn't matter since we only load dims
    else:
        raise ValueError(f"Invalid mode: {mode}. Choose 'backfill', 'incremental', or 'dims-only'")
    
        # ========== BRONZE STAGE ==========
    if not skip_bronze:
        print(f"\n{'='*80}")
        print("STAGE 1: BRONZE INGESTION")
        print(f"{'='*80}")
        print(f"Bronze mode: {bronze_mode}")
        
        bronze_args = [
            "--mode", bronze_mode,
            "--locations", locations_path
        ]
        
        if bronze_mode == "backfill":
            bronze_args.extend(["--start-date", start_date])
            bronze_args.extend(["--end-date", end_date])
            bronze_args.append("--override")  # Override existing data in backfill
        
        success, bronze_result = run_subprocess_job(
            "jobs/bronze/run_bronze_pipeline.py",
            bronze_args,
            "Bronze",
            timeout=3600  # Longer timeout for backfill
        )
        
        results["stages"]["bronze"] = bronze_result
        if not success:
            print("ERROR: Bronze stage failed")
            return {"success": False, "results": results["stages"]}
    else:
        print("\nSkipping Bronze stage")
        results["stages"]["bronze"] = "skipped"
    
        # ========== SILVER STAGE ==========
    if not skip_silver:
        print(f"\n{'='*80}")
        print("STAGE 2: SILVER TRANSFORMATION")
        print(f"{'='*80}")
        print(f"Silver mode: {silver_mode}")
        
        silver_args = ["--mode", silver_mode]
        
        if silver_mode == "full" and start_date and end_date:
            # Full mode with date range (for backfill)
            silver_args.extend(["--start-date", start_date])
            silver_args.extend(["--end-date", end_date])
        # For incremental mode, no date range needed (auto-detect)
        
        success, silver_result = run_subprocess_job(
            "jobs/silver/run_silver_pipeline.py",
            silver_args,
            "Silver",
            timeout=3600
        )
        
        results["stages"]["silver"] = silver_result
        if not success:
            print("ERROR: Silver stage failed")
            return {"success": False, "results": results["stages"]}
    else:
        print("\nSkipping Silver stage")
        results["stages"]["silver"] = "skipped"
    
    # ========== GOLD STAGE ==========
    if not skip_gold:
        print(f"\n{'='*80}")
        print("STAGE 3: GOLD LAYER")
        print(f"{'='*80}")
        
        # Step 3a: Load/update dim_date (incremental from silver)
        print("\n--- Updating dim_date (incremental from silver) ---")
        dim_date_mode = "full" if mode == "backfill" else "incremental"
        
        success, dim_date_result = run_subprocess_job(
            "jobs/gold/load_dim_date.py",
            ["--mode", dim_date_mode],
            "Gold-DimDate",
            timeout=600
        )
        
        results["stages"]["gold_dim_date"] = dim_date_result
        if not success:
            print("WARNING: dim_date update failed (continuing anyway)")
        
        # Note: Other dimensions (location, pollutant, time) are static and pre-loaded
        print("\nNote: Static dimensions (location, pollutant, time) should be pre-loaded")
        print("Run manually if needed:")
        print("  bash scripts/spark_submit.sh jobs/gold/load_dim_location.py")
        print("  bash scripts/spark_submit.sh jobs/gold/load_dim_pollutant.py")
        print("  bash scripts/spark_submit.sh jobs/gold/load_dim_time.py")
        
        # Step 3b: Transform facts (skip if dims-only mode)
        if mode != "dims-only":
            print("\n--- Transforming Gold Facts ---")
            print(f"Load mode: {gold_load_mode}")
            
            # Determine mode flag for fact scripts
            fact_mode = "full" if gold_load_mode == "full" else "incremental"
            
            # 3b.1: Transform fact_air_quality_hourly
            print("\n  [1/3] Transform fact_air_quality_hourly...")
            hourly_args = ["--mode", fact_mode]
            if fact_mode == "full" and start_date and end_date:
                hourly_args.extend(["--start-date", start_date, "--end-date", end_date])
            
            success, hourly_result = run_subprocess_job(
                "jobs/gold/transform_fact_hourly.py",
                hourly_args,
                "Gold-FactHourly",
                timeout=1800
            )
            
            results["stages"]["gold_fact_hourly"] = hourly_result
            if not success:
                print("ERROR: Gold fact_hourly transformation failed")
                return {"success": False, "results": results["stages"]}
            
            # 3b.2: Transform fact_city_daily
            print("\n  [2/3] Transform fact_city_daily...")
            daily_args = ["--mode", fact_mode]
            if fact_mode == "full" and start_date and end_date:
                daily_args.extend(["--start-date", start_date, "--end-date", end_date])
            
            success, daily_result = run_subprocess_job(
                "jobs/gold/transform_fact_daily.py",
                daily_args,
                "Gold-FactDaily",
                timeout=1800
            )
            
            results["stages"]["gold_fact_daily"] = daily_result
            if not success:
                print("ERROR: Gold fact_daily transformation failed")
                return {"success": False, "results": results["stages"]}
            
            # 3b.3: Detect episodes
            print("\n  [3/3] Detect episodes...")
            episode_args = [
                "--mode", fact_mode,
                "--aqi-threshold", str(aqi_threshold),
                "--min-hours", str(min_hours)
            ]
            if fact_mode == "full" and start_date and end_date:
                episode_args.extend(["--start-date", start_date, "--end-date", end_date])
            
            success, episode_result = run_subprocess_job(
                "jobs/gold/detect_episodes.py",
                episode_args,
                "Gold-Episodes",
                timeout=1800
            )
            
            results["stages"]["gold_episodes"] = episode_result
            if not success:
                print("ERROR: Gold episode detection failed")
                return {"success": False, "results": results["stages"]}
            
            print("\n✓ All Gold facts transformed successfully")
        else:
            print("\n--- Skipping Gold Facts (dims-only mode) ---")
            results["stages"]["gold_fact_hourly"] = "skipped"
            results["stages"]["gold_fact_daily"] = "skipped"
            results["stages"]["gold_episodes"] = "skipped"
    else:
        print("\nSkipping Gold stage")
        results["stages"]["gold"] = "skipped"
    
    # ========== SUMMARY ==========
    elapsed = time.time() - pipeline_start
    
    print(f"\n{'='*80}")
    print("FULL PIPELINE COMPLETE")
    print(f"{'='*80}")
    print(f"Mode: {mode}")
    if start_date and end_date:
        print(f"Date range: {start_date} to {end_date}")
    
    print("\nStage results:")
    all_success = True
    for stage, result in results["stages"].items():
        if isinstance(result, dict):
            status = "OK" if result.get("success") else "FAILED"
            if not result.get("success"):
                all_success = False
            elapsed_s = result.get("elapsed_seconds", 0)
            print(f"  [{status}] {stage.upper():15s} {elapsed_s:6.1f}s")
        else:
            print(f"  [SKIP] {stage.upper():15s}")
    
    print(f"\nTotal pipeline time: {elapsed:.1f}s ({elapsed/60:.1f} minutes)")
    print("="*80)
    
    return {
        "success": all_success,
        "mode": mode,
        "start_date": start_date,
        "end_date": end_date,
        "results": results["stages"],
        "elapsed_seconds": elapsed,
        "timestamp": datetime.now().isoformat()
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Full Pipeline Prefect Flow",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill 1 month of data
  python Prefect/full_pipeline_flow.py --mode backfill --start-date 2024-10-01 --end-date 2024-10-31
  
  # Incremental update (for hourly scheduling)
  python Prefect/full_pipeline_flow.py --mode incremental
  
  # Load dimensions only
  python Prefect/full_pipeline_flow.py --mode dims-only
        """
    )
    
    # Pipeline mode
    parser.add_argument(
        "--mode",
        choices=["backfill", "incremental", "dims-only"],
        default="incremental",
        help="Pipeline mode: backfill (load historical data), incremental (update from latest), dims-only (load dimensions)"
    )
    
    # Date range (for backfill mode)
    parser.add_argument("--start-date", help="Start date for backfill (YYYY-MM-DD)")
    parser.add_argument("--end-date", help="End date for backfill (YYYY-MM-DD)")
    
    # Control flags
    parser.add_argument("--skip-bronze", action="store_true", help="Skip bronze ingestion")
    parser.add_argument("--skip-silver", action="store_true", help="Skip silver transformation")
    parser.add_argument("--skip-gold", action="store_true", help="Skip gold layer")
    parser.add_argument("--load-dims", action="store_true", help="Force load gold dimensions")
    
    # Gold parameters
    parser.add_argument("--aqi-threshold", type=int, default=151, help="AQI threshold for episodes (default: 151)")
    parser.add_argument("--min-hours", type=int, default=4, help="Minimum hours for episodes (default: 4)")
    
    # Common paths
    parser.add_argument(
        "--locations",
        default="hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl",
        help="Path to locations JSONL file"
    )
    parser.add_argument(
        "--pollutants",
        default="hdfs://khoa-master:9000/user/dlhnhom2/data/dim_pollutant.jsonl",
        help="Path to pollutants JSONL file"
    )
    parser.add_argument(
        "--warehouse",
        default=os.getenv("WAREHOUSE_URI", "hdfs://khoa-master:9000/warehouse/iceberg"),
        help="Iceberg warehouse path"
    )
    
    args = parser.parse_args()
    
    # Run full pipeline
    result = full_pipeline_flow(
        mode=args.mode,
        start_date=args.start_date,
        end_date=args.end_date,
        skip_bronze=args.skip_bronze,
        skip_silver=args.skip_silver,
        skip_gold=args.skip_gold,
        load_dims=args.load_dims,
        aqi_threshold=args.aqi_threshold,
        min_hours=args.min_hours,
        locations_path=args.locations,
        pollutants_path=args.pollutants,
        warehouse=args.warehouse
    )
    
    sys.exit(0 if result["success"] else 1)
