"""Optimized Gold Pipeline - Simple, efficient, Prefect-ready.

Loads all dimension tables and transforms fact tables:
- Dimensions: dim_location, dim_pollutant, dim_time, dim_date
- Facts: fact_air_quality_hourly, fact_city_daily, fact_episode

Usage:
  python3 jobs/gold/run_gold_pipeline.py --mode [dims|facts|all]
  
  # Load all dimensions and facts
  bash scripts/spark_submit.sh jobs/gold/run_gold_pipeline.py -- --mode all
  
  # Load only dimensions
  bash scripts/spark_submit.sh jobs/gold/run_gold_pipeline.py -- --mode dims
  
  # Load only facts
  bash scripts/spark_submit.sh jobs/gold/run_gold_pipeline.py -- --mode facts
  
  # Custom: specific dimensions and facts
  bash scripts/spark_submit.sh jobs/gold/run_gold_pipeline.py -- \
    --mode custom \
    --dims location,pollutant \
    --facts hourly,daily
"""
import argparse
import os
import sys
import time
from typing import Dict, Optional

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
sys.path.insert(0, SRC_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT_DIR, ".env"))

from pyspark.sql import SparkSession


def build_spark_session(app_name: str = "run_gold_pipeline") -> SparkSession:
    from lakehouse_aqi import spark_session
    mode = "cluster" if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME") else "local"
    return spark_session.build(app_name=app_name, mode=mode)


def execute_gold_pipeline(
    mode: str = "all",
    dims_to_load: str = "",
    facts_to_load: str = "",
    locations_path: str = "hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl",
    pollutants_path: str = "hdfs://khoa-master:9000/user/dlhnhom2/data/dim_pollutant.jsonl",
    warehouse: str = "hdfs://khoa-master:9000/warehouse/iceberg",
    aqi_threshold: int = 151,
    min_hours: int = 4
) -> Dict:
    """Prefect-friendly gold pipeline: dimensions + facts."""
    try:
        spark = build_spark_session()
        spark.conf.set("spark.sql.catalog.hadoop_catalog.warehouse", warehouse)
        
        start_time = time.time()
        
        # Determine what to load
        load_dims = set()
        load_facts = set()
        
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
        
        print(f"GOLD PIPELINE")
        if load_dims:
            print(f"  Dimensions: {', '.join(sorted(load_dims))}")
        if load_facts:
            print(f"  Facts: {', '.join(sorted(load_facts))}")
        
        results = {}
        
        # === LOAD DIMENSIONS ===
        if load_dims:
            print(f"\n{'='*60}")
            print("LOADING DIMENSIONS")
            print(f"{'='*60}")
        
        if "location" in load_dims:
            from load_dim_location import load_dim_location
            print("\n--- Loading dim_location ---")
            count = load_dim_location(spark=spark, locations_path=locations_path)
            results["dim_location"] = count
        
        if "pollutant" in load_dims:
            from load_dim_pollutant import load_dim_pollutant
            print("\n--- Loading dim_pollutant ---")
            count = load_dim_pollutant(spark=spark, pollutants_path=pollutants_path)
            results["dim_pollutant"] = count
        
        if "time" in load_dims:
            from load_dim_time import generate_dim_time
            print("\n--- Generating dim_time ---")
            count = generate_dim_time(spark=spark)
            results["dim_time"] = count
        
        if "date" in load_dims:
            from load_dim_date import generate_dim_date
            print("\n--- Generating dim_date ---")
            count = generate_dim_date(spark=spark)
            results["dim_date"] = count
        
        # === TRANSFORM FACTS ===
        if load_facts:
            print(f"\n{'='*60}")
            print("TRANSFORMING FACTS")
            print(f"{'='*60}")
        
        if "hourly" in load_facts:
            from transform_fact_hourly import transform_fact_hourly
            print("\n--- Transforming fact_air_quality_hourly ---")
            result = transform_fact_hourly(spark=spark, mode="overwrite")
            results["fact_hourly"] = result.get("records_processed", 0)
        
        if "daily" in load_facts:
            from transform_fact_daily import transform_fact_daily
            print("\n--- Transforming fact_city_daily ---")
            result = transform_fact_daily(spark=spark, mode="overwrite")
            results["fact_daily"] = result.get("records_processed", 0)
        
        if "episode" in load_facts:
            from detect_episodes import detect_episodes
            print("\n--- Detecting episodes ---")
            result = detect_episodes(
                spark=spark,
                aqi_threshold=aqi_threshold,
                min_hours=min_hours,
                mode="overwrite"
            )
            results["fact_episode"] = result.get("episodes_detected", 0)
        
        elapsed = time.time() - start_time
        
        print(f"\n{'='*60}")
        print("GOLD PIPELINE COMPLETED")
        print(f"{'='*60}")
        for key, value in results.items():
            print(f"  {key}: {value} records")
        print(f"  Total time: {elapsed:.1f}s")
        print(f"{'='*60}")
        
        spark.stop()
        
        return {
            "success": True,
            "stats": results,
            "elapsed_seconds": elapsed
        }
    except Exception as e:
        print(f"ERROR: {e}")
        import traceback
        traceback.print_exc()
        return {"success": False, "error": str(e)}




def main():
    parser = argparse.ArgumentParser(description="Gold Pipeline: Load dimensions + transform facts")
    parser.add_argument("--mode", default="all", 
                        help="Pipeline mode: all|dims|facts|custom (default: all)")
    parser.add_argument("--dims", default="", 
                        help="Comma-separated dimensions (for custom mode): location,pollutant,time,date")
    parser.add_argument("--facts", default="", 
                        help="Comma-separated facts (for custom mode): hourly,daily,episode")
    parser.add_argument("--locations", default="hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl")
    parser.add_argument("--pollutants", default="hdfs://khoa-master:9000/user/dlhnhom2/data/dim_pollutant.jsonl")
    parser.add_argument("--warehouse", default="hdfs://khoa-master:9000/warehouse/iceberg")
    parser.add_argument("--aqi-threshold", type=int, default=151, 
                        help="AQI threshold for episode detection (default: 151)")
    parser.add_argument("--min-hours", type=int, default=4, 
                        help="Minimum consecutive hours for episode (default: 4)")
    
    args = parser.parse_args()
    
    result = execute_gold_pipeline(
        mode=args.mode,
        dims_to_load=args.dims,
        facts_to_load=args.facts,
        locations_path=args.locations,
        pollutants_path=args.pollutants,
        warehouse=args.warehouse,
        aqi_threshold=args.aqi_threshold,
        min_hours=args.min_hours
    )
    
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()

