"""Optimized Bronze Ingestion - Simple, efficient, Prefect-ready."""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
sys.path.insert(0, SRC_DIR)

from dotenv import load_dotenv
load_dotenv(os.path.join(ROOT_DIR, ".env"))

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, TimestampType, DateType
)

try:
    import openmeteo_requests
    import pandas as pd
    import requests_cache
    from retry_requests import retry
except ImportError:
    print("ERROR: pip install openmeteo-requests pandas requests-cache retry-requests")
    sys.exit(1)


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
    from lakehouse_aqi import spark_session
    mode = "cluster" if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME") else "local"
    return spark_session.build(app_name=app_name, mode=mode)


def load_locations(path: str) -> List[Dict]:
    with open(path, "r") as f:
        content = f.read().strip()
    
    if content.startswith('{') and '\n' in content:
        return [json.loads(line) for line in content.splitlines() if line.strip()]
    
    data = json.loads(content)
    return data if isinstance(data, list) else data.get("locations", [])


def get_latest_timestamp(spark: SparkSession, table: str, location_key: str) -> Optional[datetime]:
    try:
        result = spark.sql(f"SELECT MAX(ts_utc) as max_ts FROM {table} WHERE location_key = '{location_key}'").collect()
        return result[0]["max_ts"] if result and result[0]["max_ts"] else None
    except:
        return None


def fetch_openmeteo_data(lat: float, lon: float, start: str, end: str) -> Optional[pd.DataFrame]:
    cache = requests_cache.CachedSession('.cache', expire_after=3600)
    client = openmeteo_requests.Client(session=retry(cache, retries=5, backoff_factor=0.2))
    
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": [
            "pm2_5", "pm10", "nitrogen_dioxide", "ozone", "sulphur_dioxide", "carbon_monoxide",
            "aerosol_optical_depth", "dust", "uv_index", "carbon_dioxide",
            "us_aqi", "us_aqi_pm2_5", "us_aqi_pm10", "us_aqi_nitrogen_dioxide", 
            "us_aqi_ozone", "us_aqi_sulphur_dioxide", "us_aqi_carbon_monoxide"
        ],
        "timezone": "UTC",
        "start_date": start,
        "end_date": end,
    }
    
    try:
        response = client.weather_api("https://air-quality-api.open-meteo.com/v1/air-quality", params=params)[0]
        hourly = response.Hourly()
        
        df = pd.DataFrame({
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
        })
        
        df["latitude"] = response.Latitude()
        df["longitude"] = response.Longitude()
        df["model_domain"] = "CAMS"
        df["request_timezone"] = "UTC"
        df["date_utc"] = df["ts_utc"].dt.date
        
        return df
    except Exception as e:
        print(f"API error: {e}")
        return None


def generate_date_chunks(start: str, end: str, days: int = 90) -> List[tuple]:
    s = datetime.strptime(start, "%Y-%m-%d")
    e = datetime.strptime(end, "%Y-%m-%d")
    
    chunks = []
    current = s
    
    while current <= e:
        chunk_end = min(current + timedelta(days=days - 1), e)
        chunks.append((current.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        current = chunk_end + timedelta(days=1)
    
    return chunks


def ingest_location_chunk(spark: SparkSession, location: Dict, start: str, end: str, 
                          table: str, override: bool = False) -> int:
    loc_key = location.get("location_key") or location.get("id") or location.get("name")
    loc_name = location.get("location_name") or location.get("name", "Unknown")
    
    print(f"  {loc_name} ({start} to {end})")
    
    if not override:
        try:
            count = spark.sql(f"""
                SELECT COUNT(*) as cnt FROM {table}
                WHERE location_key = '{loc_key}' 
                AND date_utc BETWEEN '{start}' AND '{end}'
            """).collect()[0]["cnt"]
            if count > 0:
                print(f"    Skipped (exists)")
                return 0
        except:
            pass
    
    pdf = fetch_openmeteo_data(location.get("latitude"), location.get("longitude"), start, end)
    
    if pdf is None or pdf.empty:
        return 0
    
    pdf["location_key"] = loc_key
    pdf["_ingested_at"] = pd.Timestamp.now(tz="UTC")
    pdf["ts_utc"] = pd.to_datetime(pdf["ts_utc"], utc=True)
    pdf["date_utc"] = pd.to_datetime(pdf["date_utc"])
    
    for col in ["aqi", "aqi_pm25", "aqi_pm10", "aqi_no2", "aqi_o3", "aqi_so2", "aqi_co"]:
        pdf[col] = pdf[col].round().astype('Int64')
    
    pdf = pdf.where(pdf.notnull(), None)
    pdf = pdf[[field.name for field in BRONZE_SCHEMA.fields]]
    
    # Deduplicate in pandas to avoid Spark optimizer issues with Iceberg
    pdf = pdf.drop_duplicates(subset=["location_key", "ts_utc"], keep="first")
    
    sdf = spark.createDataFrame(pdf, schema=BRONZE_SCHEMA)
    
    try:
        if override:
            spark.sql(f"""
                DELETE FROM {table}
                WHERE location_key = '{loc_key}'
                AND date_utc BETWEEN '{start}' AND '{end}'
            """)
        
        sdf.write.format("iceberg").mode("append").save(table)
        rows = len(pdf)
        print(f"    Inserted {rows} rows")
        return rows
    except Exception as e:
        print(f"    Error: {e}")
        return 0


def run_backfill(spark: SparkSession, locations: List[Dict], start: str, end: str, 
                 table: str, chunk_days: int = 90, override: bool = False) -> Dict:
    print(f"BACKFILL: {start} to {end}, {len(locations)} locations")
    
    chunks = generate_date_chunks(start, end, chunk_days)
    total_rows = 0
    
    for location in locations:
        for chunk_start, chunk_end in chunks:
            rows = ingest_location_chunk(spark, location, chunk_start, chunk_end, table, override)
            total_rows += rows
            time.sleep(1)
    
    return {"total_rows": total_rows, "total_chunks": len(locations) * len(chunks)}


def run_upsert(spark: SparkSession, locations: List[Dict], table: str, 
               lookback_days: int = 7) -> Dict:
    """
    Upsert mode: Find latest data in bronze, backfill from that point to today.
    If no data exists in bronze, exit early without ingesting.
    
    Args:
        lookback_days: Not used in new logic, kept for backward compatibility
    """
    print(f"UPSERT: {len(locations)} locations")
    
    total_rows = 0
    today = datetime.now().strftime("%Y-%m-%d")
    locations_with_data = 0
    
    for location in locations:
        loc_key = location.get("location_key") or location.get("id") or location.get("name")
        loc_name = location.get("location_name") or location.get("name", "Unknown")
        
        # Find latest timestamp in bronze
        latest = get_latest_timestamp(spark, table, loc_key)
        
        if latest is None:
            print(f"  {loc_name}: No existing data in bronze, skipping upsert")
            continue
        
        locations_with_data += 1
        
        # Backfill from latest date to today
        latest_date = latest.strftime("%Y-%m-%d")
        print(f"  {loc_name}: Latest data {latest_date}, updating to {today}")
        
        if latest_date >= today:
            print(f"    Already up to date, skipping")
            continue
        
        # Calculate start date (day after latest)
        start = (latest + timedelta(days=1)).strftime("%Y-%m-%d")
        
        rows = ingest_location_chunk(spark, location, start, today, table, override=False)
        total_rows += rows
        time.sleep(1)
    
    print(f"\nUpsert summary: {locations_with_data}/{len(locations)} locations had existing data")
    return {"total_rows": total_rows, "locations_processed": locations_with_data}


def execute_ingestion(mode: str, locations_path: str, start_date: Optional[str] = None,
                      end_date: Optional[str] = None, chunk_days: int = 90, 
                      lookback_days: int = 7, override: bool = False,
                      table: str = "hadoop_catalog.lh.bronze.open_meteo_hourly",
                      warehouse: str = "hdfs://khoa-master:9000/warehouse/iceberg") -> Dict:
    """Prefect-friendly ingestion function."""
    try:
        locations = load_locations(locations_path)
        print(f"Loaded {len(locations)} locations")
        
        spark = build_spark_session()
        spark.conf.set("spark.sql.catalog.hadoop_catalog.warehouse", warehouse)
        
        start_time = time.time()
        
        if mode == "backfill":
            stats = run_backfill(spark, locations, start_date, 
                               end_date or datetime.now().strftime("%Y-%m-%d"), 
                               table, chunk_days, override)
        else:
            stats = run_upsert(spark, locations, table, lookback_days)
        
        elapsed = time.time() - start_time
        
        print(f"\nCOMPLETE: {stats['total_rows']} rows in {elapsed:.1f}s")
        
        try:
            count = spark.sql(f"SELECT COUNT(*) as cnt FROM {table}").collect()[0]["cnt"]
            print(f"Total in table: {count}")
        except:
            pass
        
        spark.stop()
        
        return {"success": True, "stats": stats, "elapsed_seconds": elapsed}
    except Exception as e:
        print(f"ERROR: {e}")
        return {"success": False, "error": str(e)}


def main():
    parser = argparse.ArgumentParser(description="Optimized Bronze Ingestion")
    parser.add_argument("--mode", choices=["backfill", "upsert"], required=True)
    parser.add_argument("--locations", default=os.path.join(ROOT_DIR, "data", "locations.jsonl"))
    parser.add_argument("--start-date")
    parser.add_argument("--end-date")
    parser.add_argument("--chunk-days", type=int, default=90)
    parser.add_argument("--lookback-days", type=int, default=7)
    parser.add_argument("--override", action="store_true")
    parser.add_argument("--table", default="hadoop_catalog.lh.bronze.open_meteo_hourly")
    parser.add_argument("--warehouse", default=os.getenv("WAREHOUSE_URI", "hdfs://khoa-master:9000/warehouse/iceberg"))
    
    args = parser.parse_args()
    
    if args.mode == "backfill" and not args.start_date:
        parser.error("--start-date required for backfill")
    
    result = execute_ingestion(
        mode=args.mode,
        locations_path=args.locations,
        start_date=args.start_date,
        end_date=args.end_date,
        chunk_days=args.chunk_days,
        lookback_days=args.lookback_days,
        override=args.override,
        table=args.table,
        warehouse=args.warehouse
    )
    
    sys.exit(0 if result["success"] else 1)


if __name__ == "__main__":
    main()
