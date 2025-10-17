"""Prefect Flow for Gold Layer Pipeline.

This flow orchestrates the Gold layer pipeline (dimensions + facts) with:
- Single SparkSession per flow execution (YARN-compatible)
- Parallel execution of independent dimensions
- Sequential execution of dependent facts
- Automatic retry on failure
- Proper logging and metrics

Usage (via YARN):
  bash scripts/spark_submit.sh Prefect/gold_flow.py -- --mode all
  bash scripts/spark_submit.sh Prefect/gold_flow.py -- --mode dims
  bash scripts/spark_submit.sh Prefect/gold_flow.py -- --mode facts
  bash scripts/spark_submit.sh Prefect/gold_flow.py -- --mode custom --dims location,pollutant --facts hourly

DO NOT run directly with python - use spark_submit.sh wrapper for YARN deployment.
"""
import argparse
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional, Set

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
    name="Load Dimension Location",
    description="Load location dimension from HDFS",
    retries=2,
    retry_delay_seconds=30,
    log_prints=True
)
def load_dim_location_task(locations_path: str) -> int:
    """Load location dimension."""
    from gold.load_dim_location import load_dim_location
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    print(f"Loading dim_location from {locations_path}")
    count = load_dim_location(spark=spark, locations_path=locations_path)
    print(f"Loaded {count} locations")
    
    return count


@task(
    name="Load Dimension Pollutant",
    description="Load pollutant dimension from HDFS",
    retries=2,
    retry_delay_seconds=30,
    log_prints=True
)
def load_dim_pollutant_task(pollutants_path: str) -> int:
    """Load pollutant dimension."""
    from gold.load_dim_pollutant import load_dim_pollutant
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    print(f"Loading dim_pollutant from {pollutants_path}")
    count = load_dim_pollutant(spark=spark, pollutants_path=pollutants_path)
    print(f"Loaded {count} pollutants")
    
    return count


@task(
    name="Generate Dimension Time",
    description="Auto-generate time dimension (24 hours)",
    retries=2,
    retry_delay_seconds=30,
    log_prints=True
)
def generate_dim_time_task() -> int:
    """Generate time dimension."""
    from gold.load_dim_time import generate_dim_time
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    print("Generating dim_time (24 hours)")
    count = generate_dim_time(spark=spark)
    print(f"Generated {count} time records")
    
    return count


@task(
    name="Generate Dimension Date",
    description="Extract date dimension from silver layer",
    retries=2,
    retry_delay_seconds=30,
    log_prints=True
)
def generate_dim_date_task() -> int:
    """Generate date dimension."""
    from gold.load_dim_date import generate_dim_date
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    print("Generating dim_date from silver layer")
    count = generate_dim_date(spark=spark)
    print(f"Generated {count} date records")
    
    return count


@task(
    name="Transform Fact Hourly",
    description="Transform fact_air_quality_hourly from silver + dimensions",
    retries=2,
    retry_delay_seconds=60,
    log_prints=True
)
def transform_fact_hourly_task(mode: str = "overwrite") -> Dict:
    """Transform fact_air_quality_hourly."""
    from gold.transform_fact_hourly import transform_fact_hourly
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    print(f"Transforming fact_air_quality_hourly (mode={mode})")
    result = transform_fact_hourly(
        spark=spark, 
        mode=mode,
        auto_detect=True
    )
    print(f"Processed {result.get('records_processed', 0)} records")
    
    return result


@task(
    name="Transform Fact Daily",
    description="Aggregate daily city-level facts",
    retries=2,
    retry_delay_seconds=60,
    log_prints=True
)
def transform_fact_daily_task(mode: str = "overwrite") -> Dict:
    """Transform fact_city_daily."""
    from gold.transform_fact_daily import transform_fact_daily
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    print(f"Transforming fact_city_daily (mode={mode})")
    result = transform_fact_daily(
        spark=spark, 
        mode=mode,
        auto_detect=True
    )
    print(f"Processed {result.get('records_processed', 0)} records")
    
    return result


@task(
    name="Detect Episodes",
    description="Detect pollution episodes with AQI threshold",
    retries=2,
    retry_delay_seconds=60,
    log_prints=True
)
def detect_episodes_task(
    aqi_threshold: int = 151,
    min_hours: int = 4,
    mode: str = "overwrite"
) -> Dict:
    """Detect pollution episodes."""
    from gold.detect_episodes import detect_episodes
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.getOrCreate()
    
    print(f"Detecting episodes (threshold={aqi_threshold}, min_hours={min_hours})")
    result = detect_episodes(
        spark=spark,
        aqi_threshold=aqi_threshold,
        min_hours=min_hours,
        mode=mode
    )
    print(f"Detected {result.get('episodes_detected', 0)} episodes")
    
    return result


@flow(
    name="Gold Pipeline Flow",
    description="Load dimensions and transform facts for Gold layer",
    log_prints=True
)
def gold_pipeline_flow(
    mode: str = "all",
    dims_to_load: Optional[str] = None,
    facts_to_load: Optional[str] = None,
    locations_path: str = "hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl",
    pollutants_path: str = "hdfs://khoa-master:9000/user/dlhnhom2/data/dim_pollutant.jsonl",
    warehouse: str = "hdfs://khoa-master:9000/warehouse/iceberg",
    aqi_threshold: int = 151,
    min_hours: int = 4,
    require_yarn: bool = True
) -> Dict:
    """Gold pipeline flow with single SparkSession.
    
    Args:
        mode: 'all', 'dims', 'facts', or 'custom'
        dims_to_load: Comma-separated dimensions for custom mode
        facts_to_load: Comma-separated facts for custom mode
        locations_path: Path to locations configuration
        pollutants_path: Path to pollutants configuration
        warehouse: Iceberg warehouse URI
        aqi_threshold: AQI threshold for episode detection
        min_hours: Minimum hours for episode
        require_yarn: Validate YARN deployment
        
    Returns:
        Dictionary with flow statistics
    """
    print("="*80)
    print("GOLD PIPELINE FLOW")
    print("="*80)
    print(f"Mode: {mode}")
    
    flow_start = time.time()
    
    # Determine what to load
    load_dims: Set[str] = set()
    load_facts: Set[str] = set()
    
    if mode == "all":
        load_dims = {"location", "pollutant", "time", "date"}
        load_facts = {"hourly", "daily", "episode"}
    elif mode == "dims":
        load_dims = {"location", "pollutant", "time", "date"}
    elif mode == "facts":
        load_facts = {"hourly", "daily", "episode"}
    elif mode == "custom":
        if dims_to_load:
            load_dims = set(d.strip().lower() for d in dims_to_load.split(","))
        if facts_to_load:
            load_facts = set(f.strip().lower() for f in facts_to_load.split(","))
    
    if load_dims:
        print(f"Dimensions: {', '.join(sorted(load_dims))}")
    if load_facts:
        print(f"Facts: {', '.join(sorted(load_facts))}")
    
    # Create single Spark session for entire flow
    with get_spark_session(
        app_name="gold_pipeline_flow",
        require_yarn=require_yarn
    ) as spark:
        
        # Validate YARN if required
        if require_yarn:
            validate_yarn_mode(spark)
        
        # Log Spark info
        log_spark_info(spark, "Gold Flow")
        
        # Set warehouse
        spark.conf.set("spark.sql.catalog.hadoop_catalog.warehouse", warehouse)
        
        results = {}
        
        # === LOAD DIMENSIONS ===
        if load_dims:
            print(f"\n{'='*60}")
            print("LOADING DIMENSIONS")
            print(f"{'='*60}\n")
            
            if "location" in load_dims:
                count = load_dim_location_task(locations_path)
                results["dim_location"] = count
            
            if "pollutant" in load_dims:
                count = load_dim_pollutant_task(pollutants_path)
                results["dim_pollutant"] = count
            
            if "time" in load_dims:
                count = generate_dim_time_task()
                results["dim_time"] = count
            
            if "date" in load_dims:
                count = generate_dim_date_task()
                results["dim_date"] = count
        
        # === TRANSFORM FACTS ===
        if load_facts:
            print(f"\n{'='*60}")
            print("TRANSFORMING FACTS")
            print(f"{'='*60}\n")
            
            if "hourly" in load_facts:
                result = transform_fact_hourly_task(mode="overwrite")
                results["fact_hourly"] = result.get("records_processed", 0)
            
            if "daily" in load_facts:
                result = transform_fact_daily_task(mode="overwrite")
                results["fact_daily"] = result.get("records_processed", 0)
            
            if "episode" in load_facts:
                result = detect_episodes_task(
                    aqi_threshold=aqi_threshold,
                    min_hours=min_hours,
                    mode="overwrite"
                )
                results["fact_episode"] = result.get("episodes_detected", 0)
        
        elapsed = time.time() - flow_start
        
        # Calculate totals
        total_records = sum(results.values())
        
        print(f"\n{'='*80}")
        print("GOLD FLOW COMPLETE")
        print(f"{'='*80}")
        for key, value in sorted(results.items()):
            print(f"  {key}: {value:,} records")
        print(f"\nTotal records: {total_records:,}")
        print(f"Elapsed time: {elapsed:.1f}s")
        
        return {
            "success": True,
            "mode": mode,
            "results": results,
            "total_records": total_records,
            "elapsed_seconds": elapsed
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gold Pipeline Prefect Flow - Submit via spark_submit.sh"
    )
    parser.add_argument(
        "--mode",
        choices=["all", "dims", "facts", "custom"],
        default="all",
        help="Pipeline mode: all, dims only, facts only, or custom"
    )
    parser.add_argument(
        "--dims",
        help="Comma-separated dimensions for custom mode: location,pollutant,time,date"
    )
    parser.add_argument(
        "--facts",
        help="Comma-separated facts for custom mode: hourly,daily,episode"
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
        "--aqi-threshold",
        type=int,
        default=151,
        help="AQI threshold for episode detection"
    )
    parser.add_argument(
        "--min-hours",
        type=int,
        default=4,
        help="Minimum consecutive hours for episode"
    )
    parser.add_argument(
        "--no-yarn-check",
        action="store_true",
        help="Skip YARN validation (for local testing only)"
    )
    
    args = parser.parse_args()
    
    # Run flow
    result = gold_pipeline_flow(
        mode=args.mode,
        dims_to_load=args.dims,
        facts_to_load=args.facts,
        locations_path=args.locations,
        pollutants_path=args.pollutants,
        warehouse=args.warehouse,
        aqi_threshold=args.aqi_threshold,
        min_hours=args.min_hours,
        require_yarn=not args.no_yarn_check
    )
    
    sys.exit(0 if result["success"] else 1)
