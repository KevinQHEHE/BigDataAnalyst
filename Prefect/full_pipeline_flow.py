"""Prefect Full Pipeline Flow: Bronze → Silver → Gold.

This flow orchestrates the complete data pipeline with:
- Single SparkSession for all stages (YARN-compatible)
- Sequential execution: Bronze → Silver → Gold
- Comprehensive metrics collection
- Automatic retry and error handling
- Proper logging

Usage (via YARN):
  # Run full pipeline (upsert bronze, incremental silver, all gold)
  bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py

  # Run with specific date range for silver
  bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --start-date 2024-01-01 --end-date 2024-12-31

  # Skip bronze ingestion (only silver + gold)
  bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --skip-bronze

DO NOT run directly with python - use spark_submit.sh wrapper for YARN deployment.
"""
import argparse
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

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

# Import individual flows
from bronze_flow import bronze_ingestion_flow
from silver_flow import silver_transformation_flow
from gold_flow import gold_pipeline_flow


@flow(
    name="Full Pipeline Flow",
    description="Complete Bronze → Silver → Gold pipeline with single SparkSession",
    log_prints=True
)
def full_pipeline_flow(
    # Bronze parameters
    bronze_mode: str = "upsert",
    bronze_start_date: Optional[str] = None,
    bronze_end_date: Optional[str] = None,
    skip_bronze: bool = False,
    
    # Silver parameters
    silver_mode: str = "incremental",
    silver_start_date: Optional[str] = None,
    silver_end_date: Optional[str] = None,
    skip_validation: bool = False,
    skip_silver: bool = False,
    
    # Gold parameters
    gold_mode: str = "all",
    skip_gold: bool = False,
    aqi_threshold: int = 151,
    min_hours: int = 4,
    
    # Common parameters
    locations_path: str = "hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl",
    pollutants_path: str = "hdfs://khoa-master:9000/user/dlhnhom2/data/dim_pollutant.jsonl",
    warehouse: str = "hdfs://khoa-master:9000/warehouse/iceberg",
    require_yarn: bool = True
) -> Dict:
    """Execute complete Bronze → Silver → Gold pipeline.
    
    Args:
        bronze_mode: 'backfill' or 'upsert'
        bronze_start_date: Start date for bronze backfill
        bronze_end_date: End date for bronze backfill
        skip_bronze: Skip bronze ingestion
        
        silver_mode: 'full' or 'incremental'
        silver_start_date: Start date for silver transformation
        silver_end_date: End date for silver transformation
        skip_validation: Skip silver validation
        skip_silver: Skip silver transformation
        
        gold_mode: 'all', 'dims', 'facts', or 'custom'
        skip_gold: Skip gold pipeline
        aqi_threshold: AQI threshold for episode detection
        min_hours: Minimum hours for episode
        
        locations_path: Path to locations configuration
        pollutants_path: Path to pollutants configuration
        warehouse: Iceberg warehouse URI
        require_yarn: Validate YARN deployment
        
    Returns:
        Dictionary with pipeline statistics
    """
    print("="*80)
    print("FULL PIPELINE FLOW: BRONZE → SILVER → GOLD")
    print("="*80)
    print(f"Pipeline stages:")
    print(f"  Bronze: {'SKIP' if skip_bronze else bronze_mode.upper()}")
    print(f"  Silver: {'SKIP' if skip_silver else silver_mode.upper()}")
    print(f"  Gold: {'SKIP' if skip_gold else gold_mode.upper()}")
    print("="*80)
    
    pipeline_start = time.time()
    results = {}
    
    # Create single Spark session for entire pipeline
    with get_spark_session(
        app_name="full_pipeline_flow",
        require_yarn=require_yarn
    ) as spark:
        
        # Validate YARN if required
        if require_yarn:
            validate_yarn_mode(spark)
        
        # Log Spark info
        log_spark_info(spark, "Full Pipeline")
        
        # Set warehouse
        spark.conf.set("spark.sql.catalog.hadoop_catalog.warehouse", warehouse)
        
        # === STAGE 1: BRONZE ===
        if not skip_bronze:
            print(f"\n{'='*80}")
            print("STAGE 1/3: BRONZE INGESTION")
            print(f"{'='*80}\n")
            
            try:
                bronze_result = bronze_ingestion_flow(
                    mode=bronze_mode,
                    locations_path=locations_path,
                    start_date=bronze_start_date,
                    end_date=bronze_end_date,
                    table="hadoop_catalog.lh.bronze.open_meteo_hourly",
                    warehouse=warehouse,
                    require_yarn=False  # Already validated
                )
                results["bronze"] = bronze_result
                print(f"✓ Bronze stage complete: {bronze_result.get('total_rows', 0)} rows")
            except Exception as e:
                print(f"✗ Bronze stage failed: {e}")
                results["bronze"] = {"success": False, "error": str(e)}
                # Continue to next stages even if bronze fails
        else:
            print(f"\n⊘ Skipping Bronze stage")
            results["bronze"] = "skipped"
        
        # === STAGE 2: SILVER ===
        if not skip_silver:
            print(f"\n{'='*80}")
            print("STAGE 2/3: SILVER TRANSFORMATION")
            print(f"{'='*80}\n")
            
            try:
                silver_result = silver_transformation_flow(
                    mode=silver_mode,
                    start_date=silver_start_date,
                    end_date=silver_end_date,
                    skip_validation=skip_validation,
                    bronze_table="hadoop_catalog.lh.bronze.open_meteo_hourly",
                    silver_table="hadoop_catalog.lh.silver.air_quality_hourly_clean",
                    warehouse=warehouse,
                    require_yarn=False  # Already validated
                )
                results["silver"] = silver_result
                records = silver_result.get("transformation", {}).get("records_processed", 0)
                print(f"✓ Silver stage complete: {records} rows")
            except Exception as e:
                print(f"✗ Silver stage failed: {e}")
                results["silver"] = {"success": False, "error": str(e)}
                # Continue to gold even if silver fails (gold might use existing data)
        else:
            print(f"\n⊘ Skipping Silver stage")
            results["silver"] = "skipped"
        
        # === STAGE 3: GOLD ===
        if not skip_gold:
            print(f"\n{'='*80}")
            print("STAGE 3/3: GOLD PIPELINE")
            print(f"{'='*80}\n")
            
            try:
                gold_result = gold_pipeline_flow(
                    mode=gold_mode,
                    locations_path=locations_path,
                    pollutants_path=pollutants_path,
                    warehouse=warehouse,
                    aqi_threshold=aqi_threshold,
                    min_hours=min_hours,
                    require_yarn=False  # Already validated
                )
                results["gold"] = gold_result
                total_records = gold_result.get("total_records", 0)
                print(f"✓ Gold stage complete: {total_records} records")
            except Exception as e:
                print(f"✗ Gold stage failed: {e}")
                results["gold"] = {"success": False, "error": str(e)}
        else:
            print(f"\n⊘ Skipping Gold stage")
            results["gold"] = "skipped"
    
    # === PIPELINE SUMMARY ===
    elapsed = time.time() - pipeline_start
    
    print(f"\n{'='*80}")
    print("FULL PIPELINE COMPLETE")
    print(f"{'='*80}")
    
    # Determine success
    success = True
    for stage in ["bronze", "silver", "gold"]:
        if stage in results and isinstance(results[stage], dict):
            stage_success = results[stage].get("success", False)
            status_icon = "✓" if stage_success else "✗"
            print(f"{status_icon} {stage.upper()}: {results[stage]}")
            if not stage_success:
                success = False
        else:
            print(f"⊘ {stage.upper()}: {results.get(stage, 'unknown')}")
    
    print(f"\nTotal pipeline time: {elapsed:.1f}s")
    print(f"Overall status: {'SUCCESS' if success else 'FAILED'}")
    print("="*80)
    
    return {
        "success": success,
        "results": results,
        "elapsed_seconds": elapsed,
        "timestamp": datetime.now().isoformat()
    }


@flow(
    name="Hourly Pipeline Flow",
    description="Scheduled hourly pipeline: upsert bronze + incremental silver + update gold",
    log_prints=True
)
def hourly_pipeline_flow(
    warehouse: str = "hdfs://khoa-master:9000/warehouse/iceberg",
    require_yarn: bool = True
) -> Dict:
    """Simplified flow for hourly scheduling.
    
    This flow is optimized for hourly runs:
    - Bronze: upsert mode (update from latest to now)
    - Silver: incremental mode (merge new data)
    - Gold: update all (dims + facts)
    
    Args:
        warehouse: Iceberg warehouse URI
        require_yarn: Validate YARN deployment
        
    Returns:
        Dictionary with pipeline statistics
    """
    print("="*80)
    print("HOURLY PIPELINE FLOW")
    print(f"Timestamp: {datetime.now().isoformat()}")
    print("="*80)
    
    return full_pipeline_flow(
        bronze_mode="upsert",
        skip_bronze=False,
        silver_mode="incremental",
        skip_validation=True,  # Skip validation for speed
        skip_silver=False,
        gold_mode="all",
        skip_gold=False,
        warehouse=warehouse,
        require_yarn=require_yarn
    )


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
    silver_group.add_argument("--skip-validation", action="store_true")
    silver_group.add_argument("--skip-silver", action="store_true")
    
    # Gold options
    gold_group = parser.add_argument_group("Gold options")
    gold_group.add_argument(
        "--gold-mode",
        choices=["all", "dims", "facts", "custom"],
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
        "--no-yarn-check",
        action="store_true",
        help="Skip YARN validation (for local testing only)"
    )
    parser.add_argument(
        "--hourly",
        action="store_true",
        help="Run in hourly mode (optimized for scheduled runs)"
    )
    
    args = parser.parse_args()
    
    # Run appropriate flow
    if args.hourly:
        result = hourly_pipeline_flow(
            warehouse=args.warehouse,
            require_yarn=not args.no_yarn_check
        )
    else:
        result = full_pipeline_flow(
            bronze_mode=args.bronze_mode,
            bronze_start_date=args.bronze_start_date,
            bronze_end_date=args.bronze_end_date,
            skip_bronze=args.skip_bronze,
            silver_mode=args.silver_mode,
            silver_start_date=args.silver_start_date,
            silver_end_date=args.silver_end_date,
            skip_validation=args.skip_validation,
            skip_silver=args.skip_silver,
            gold_mode=args.gold_mode,
            skip_gold=args.skip_gold,
            aqi_threshold=args.aqi_threshold,
            min_hours=args.min_hours,
            locations_path=args.locations,
            pollutants_path=args.pollutants,
            warehouse=args.warehouse,
            require_yarn=not args.no_yarn_check
        )
    
    sys.exit(0 if result["success"] else 1)
