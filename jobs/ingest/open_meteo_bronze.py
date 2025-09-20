import argparse, json, uuid, time, os
from datetime import datetime, timedelta, timezone

import requests_cache
from retry_requests import retry
import openmeteo_requests
from pyspark.sql import functions as F, types as T

from aq_lakehouse.spark_session import build

API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
HOURLY_VARS = [
    "aerosol_optical_depth", "pm2_5", "pm10", "dust",
    "nitrogen_dioxide", "ozone", "sulphur_dioxide",
    "carbon_monoxide", "uv_index", "uv_index_clear_sky"
]

def load_locations(path):
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    # -> list of dicts: [{location_id, lat, lon}]
    return [{"location_id": k, "lat": v["latitude"], "lon": v["longitude"]} for k, v in raw.items()]

def new_client():
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    return openmeteo_requests.Client(session=retry_session)

def fetch_chunk(client, lat, lon, start_date, end_date):
    params = {
        "domains": "cams_global",
        "latitude": lat, "longitude": lon,
        "hourly": HOURLY_VARS,
        "utm_source": "aq-lakehouse",
        "start_date": start_date,    # YYYY-MM-DD
        "end_date":   end_date,      # YYYY-MM-DD
        "timezone": "UTC"
    }
    resp = client.weather_api(API_URL, params=params)
    return resp[0]  # one location per call

def response_to_rows(location_id, lat, lon, resp):
    # Build rows by iterating time range
    hourly = resp.Hourly()
    start = hourly.Time()
    end   = hourly.TimeEnd()
    step  = hourly.Interval()  # seconds
    n = int((end - start) / step)

    # map variable index by requested order
    v = [hourly.Variables(i).ValuesAsNumpy() for i in range(len(HOURLY_VARS))]

    rows = []
    for i in range(n):
        ts = datetime.fromtimestamp(start + i*step, tz=timezone.utc)
        # Gracefully turn NaN into None
        def val(a, i): 
            x = float(a[i]) if a is not None else None
            if x != x:  # NaN check
                return None
            return x
        rows.append({
            "location_id": location_id,
            "latitude": float(lat),
            "longitude": float(lon),
            "ts": ts,
            "aerosol_optical_depth": val(v[0], i),
            "pm2_5":               val(v[1], i),
            "pm10":                val(v[2], i),
            "dust":                val(v[3], i),
            "nitrogen_dioxide":    val(v[4], i),
            "ozone":               val(v[5], i),
            "sulphur_dioxide":     val(v[6], i),
            "carbon_monoxide":     val(v[7], i),
            "uv_index":            val(v[8], i),
            "uv_index_clear_sky":  val(v[9], i),
        })
    return rows

def ensure_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.aq")
    spark.sql("""
      CREATE TABLE IF NOT EXISTS hadoop_catalog.aq.raw_open_meteo_hourly (
        location_id STRING,
        latitude DOUBLE,
        longitude DOUBLE,
        ts TIMESTAMP,
        aerosol_optical_depth DOUBLE,
        pm2_5 DOUBLE,
        pm10 DOUBLE,
        dust DOUBLE,
        nitrogen_dioxide DOUBLE,
        ozone DOUBLE,
        sulphur_dioxide DOUBLE,
        carbon_monoxide DOUBLE,
        uv_index DOUBLE,
        uv_index_clear_sky DOUBLE,
        source STRING,
        run_id STRING,
        ingest_ts TIMESTAMP
      ) USING iceberg
      PARTITIONED BY (days(ts))
    """)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--locations", required=True, help="JSON file with {name:{latitude,longitude}}")
    ap.add_argument("--start-date", help="YYYY-MM-DD (UTC)", required=False)
    ap.add_argument("--end-date",   help="YYYY-MM-DD (UTC)", required=False)
    ap.add_argument("--chunk-days", type=int, default=10, help="split API calls into <=N-day chunks")
    ap.add_argument("--update-from-db", action="store_true", help="compute start date from max(ts) in DB and fetch from that date to now")
    ap.add_argument("--yes", action="store_true", help="auto-confirm prompts (useful for non-interactive/backfill)")
    args = ap.parse_args()

    # If not using --update-from-db, require explicit start/end dates
    if not args.update_from_db:
        if not args.start_date or not args.end_date:
            ap.error("the following arguments are required: --start-date, --end-date (or use --update-from-db)")
    spark = build("ingest_open_meteo_bronze")
    ensure_tables(spark)

    # If requested, compute start/end dates from DB
    if args.update_from_db:
        # check whether table exists and get max(ts)
        try:
            if spark.catalog.tableExists("hadoop_catalog.aq.raw_open_meteo_hourly"):
                row = spark.sql("SELECT MAX(ts) AS max_ts FROM hadoop_catalog.aq.raw_open_meteo_hourly").collect()
                max_ts = row[0][0] if row and row[0] and row[0][0] is not None else None
            else:
                max_ts = None
        except Exception as e:
            print(f"[WARN] failed to inspect existing table: {e}")
            max_ts = None

        if max_ts is None:
            msg = "No existing data found in hadoop_catalog.aq.raw_open_mete_hourly. Do you want to backfill from 2023-01-01 to today? [y/N]: "
            proceed = False
            if args.yes:
                proceed = True
            else:
                try:
                    ans = input(msg)
                    proceed = ans.strip().lower() in ("y","yes")
                except Exception:
                    proceed = False

            if not proceed:
                print("[INFO] Aborting because no data to update and backfill not confirmed.")
                return
            start_date = datetime.fromisoformat("2023-01-01")
            end_date = datetime.now(timezone.utc)
        else:
            # start from the next hour after max_ts to avoid duplicates
            start_date = max_ts + timedelta(seconds=1)
            end_date = datetime.now(timezone.utc)

        # Replace args.start_date/ end_date for the run
        args.start_date = start_date.strftime("%Y-%m-%d")
        args.end_date = end_date.strftime("%Y-%m-%d")

    client = new_client()
    run_id = str(uuid.uuid4())

    locations = load_locations(args.locations)
    start0 = datetime.fromisoformat(args.start_date)
    end0   = datetime.fromisoformat(args.end_date)
    rows_all = []

    for loc in locations:
        lid, lat, lon = loc["location_id"], loc["lat"], loc["lon"]
        cur = start0
        while cur <= end0:
            nxt = min(cur + timedelta(days=args.chunk_days), end0)
            try:
                resp = fetch_chunk(client, lat, lon, cur.strftime("%Y-%m-%d"), nxt.strftime("%Y-%m-%d"))
                chunk_rows = response_to_rows(lid, lat, lon, resp)
                rows_all.extend(chunk_rows)
                time.sleep(0.2)  # nhã nhặn với API
            except Exception as e:
                print(f"[WARN] {lid} {cur} -> {nxt} failed: {e}")
            cur = nxt + timedelta(days=1)

    if not rows_all:
        print("[INFO] no rows fetched.")
        return

    schema = T.StructType([
        T.StructField("location_id", T.StringType()),
        T.StructField("latitude", T.DoubleType()),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("ts", T.TimestampType()),
        T.StructField("aerosol_optical_depth", T.DoubleType()),
        T.StructField("pm2_5", T.DoubleType()),
        T.StructField("pm10", T.DoubleType()),
        T.StructField("dust", T.DoubleType()),
        T.StructField("nitrogen_dioxide", T.DoubleType()),
        T.StructField("ozone", T.DoubleType()),
        T.StructField("sulphur_dioxide", T.DoubleType()),
        T.StructField("carbon_monoxide", T.DoubleType()),
        T.StructField("uv_index", T.DoubleType()),
        T.StructField("uv_index_clear_sky", T.DoubleType()),
    ])

    df = spark.createDataFrame(rows_all, schema=schema)\
              .withColumn("source", F.lit("open-meteo"))\
              .withColumn("run_id", F.lit(run_id))\
              .withColumn("ingest_ts", F.current_timestamp())

    # Một lớp vệ sinh nhẹ cho Bronze (giữ “raw-normalized” nhưng không rác cực đoan)
    for c in ["pm2_5","pm10","dust","nitrogen_dioxide","ozone","sulphur_dioxide","carbon_monoxide",
              "aerosol_optical_depth","uv_index","uv_index_clear_sky"]:
        df = df.withColumn(c, F.when(F.col(c) < 0, None).otherwise(F.col(c)))

    # tránh trùng khi dồn nhiều chunk
    df = df.dropDuplicates(["location_id","ts"])

    df.writeTo("hadoop_catalog.aq.raw_open_meteo_hourly").append()
    spark.table("hadoop_catalog.aq.raw_open_meteo_hourly")\
         .groupBy("location_id")\
         .agg(F.min("ts").alias("min_ts"), F.max("ts").alias("max_ts"), F.count("*").alias("rows"))\
         .show(truncate=False)

    print(f"[DONE] run_id={run_id}")

if __name__ == "__main__":
    main()
