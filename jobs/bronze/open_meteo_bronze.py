import argparse
import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Iterable, List

import openmeteo_requests
import requests_cache
from pyspark.sql import functions as F, types as T
from retry_requests import retry

from aq_lakehouse.spark_session import build

API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
HOURLY_VARS = [
    "aerosol_optical_depth", "pm2_5", "pm10", "dust",
    "nitrogen_dioxide", "ozone", "sulphur_dioxide",
    "carbon_monoxide", "uv_index", "uv_index_clear_sky"
]

def load_locations(path: str) -> List[dict]:
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
    if len(v) != len(HOURLY_VARS):
        logging.warning(
            "Requested %d hourly variables but API returned %d; proceeding with available variables",
            len(HOURLY_VARS), len(v)
        )

    rows = []
    for i in range(n):
        ts = datetime.fromtimestamp(start + i*step, tz=timezone.utc)
        # Gracefully turn NaN into None and tolerate missing arrays/indexes
        def val(a, i):
            try:
                if a is None:
                    return None
                x = float(a[i])
            except (IndexError, TypeError, ValueError):
                return None
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
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.aq.bronze")
    spark.sql(
        """
      CREATE TABLE IF NOT EXISTS hadoop_catalog.aq.bronze.raw_open_meteo_hourly (
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
        ingested_at TIMESTAMP
      ) USING iceberg
      PARTITIONED BY (days(ts))
    """
    )
    spark.sql(
        """
      ALTER TABLE hadoop_catalog.aq.bronze.raw_open_meteo_hourly SET TBLPROPERTIES (
        'format-version'='2',
        'write.target-file-size-bytes'='134217728',
        'write.distribution-mode'='hash'
      )
    """
    )

    if spark.catalog.tableExists("hadoop_catalog.aq.bronze.raw_open_meteo_hourly"):
        try:
            cols = [field.name for field in spark.table("hadoop_catalog.aq.bronze.raw_open_meteo_hourly").schema]
            if "ingest_ts" in cols and "ingested_at" not in cols:
                logging.info("Renaming legacy column ingest_ts -> ingested_at")
                spark.sql(
                    "ALTER TABLE hadoop_catalog.aq.bronze.raw_open_meteo_hourly RENAME COLUMN ingest_ts TO ingested_at"
                )
        except Exception as exc:  # noqa: BLE001
            logging.warning("Failed to check/upgrade Bronze schema: %s", exc)


def maybe_delete_range(spark, start_ts: datetime, end_ts: datetime, location_ids: Iterable[str]):
    if start_ts is None or end_ts is None:
        raise ValueError("start_ts and end_ts are required for range deletion")

    # Spark SQL expects naive timestamp literals (interpreted as UTC in Iceberg)
    start_literal = start_ts.strftime("%Y-%m-%d %H:%M:%S")
    end_literal = end_ts.strftime("%Y-%m-%d %H:%M:%S")

    predicates = [f"ts >= TIMESTAMP '{start_literal}'", f"ts <= TIMESTAMP '{end_literal}'"]
    if location_ids:
        quoted = ",".join([f"'{lid}'" for lid in location_ids])
        predicates.append(f"location_id IN ({quoted})")

    condition = " AND ".join(predicates)
    logging.info("Deleting existing Bronze rows where %s", condition)
    spark.sql(
        f"""
        DELETE FROM hadoop_catalog.aq.bronze.raw_open_meteo_hourly
        WHERE {condition}
        """
    )


def run_housekeeping(spark):
    housekeeping_sql = [
        """
        CALL hadoop_catalog.system.rewrite_data_files(
          'aq.bronze.raw_open_meteo_hourly',
          map('target-file-size-bytes', CAST(134217728 AS bigint))
        )
        """,
        """
        CALL hadoop_catalog.system.expire_snapshots(
          'aq.bronze.raw_open_meteo_hourly',
          CURRENT_TIMESTAMP - INTERVAL 30 DAYS
        )
        """,
        """
        CALL hadoop_catalog.system.remove_orphan_files('aq.bronze.raw_open_meteo_hourly')
        """,
    ]

    for stmt in housekeeping_sql:
        try:
            spark.sql(stmt)
        except Exception as exc:  # noqa: BLE001 - log and continue
            logging.warning("Housekeeping statement failed: %s", exc)

def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument("--locations", required=True, help="JSON file with {name:{latitude,longitude}}")
    # Accept both --start and legacy --start-date as aliases (same dest)
    ap.add_argument("--start", "--start-date", dest="start", help="YYYY-MM-DD (UTC)")
    ap.add_argument("--end", "--end-date", dest="end", help="YYYY-MM-DD (UTC)")
    ap.add_argument("--chunk-days", type=int, default=10, help="split API calls into <=N-day chunks")
    ap.add_argument("--update-from-db", action="store_true", help="compute start date from max(ts) in DB and fetch from that date to now")
    ap.add_argument("--yes", action="store_true", help="auto-confirm prompts (useful for non-interactive/backfill)")
    ap.add_argument("--mode", choices=["upsert", "replace-range"], default="upsert", help="write mode for Bronze")
    # Note: ingest uses the list in configs/locations.json. To limit locations, edit that file.
    return ap.parse_args()


def resolve_run_window(args: argparse.Namespace, spark) -> tuple[datetime, datetime]:
    if not args.update_from_db:
        if not args.start or not args.end:
            raise SystemExit("the following arguments are required: --start, --end (or use --update-from-db)")
        start_dt = datetime.fromisoformat(args.start).replace(tzinfo=timezone.utc)
        end_dt = datetime.fromisoformat(args.end).replace(tzinfo=timezone.utc)
        return start_dt, end_dt

    try:
        if spark.catalog.tableExists("hadoop_catalog.aq.bronze.raw_open_meteo_hourly"):
            row = spark.sql(
                "SELECT MAX(ts) AS max_ts FROM hadoop_catalog.aq.bronze.raw_open_meteo_hourly"
            ).collect()
            max_ts = row[0][0] if row and row[0] and row[0][0] is not None else None
        else:
            max_ts = None
    except Exception as exc:  # noqa: BLE001 - best effort
        logging.warning("Failed to inspect Bronze table: %s", exc)
        max_ts = None

    if max_ts is None:
        msg = (
            "No existing data found in hadoop_catalog.aq.bronze.raw_open_meteo_hourly. "
            "Backfill from 2023-01-01 to today? [y/N]: "
        )
        proceed = args.yes
        if not args.yes:
            try:
                ans = input(msg)
                proceed = ans.strip().lower() in ("y", "yes")
            except Exception:  # noqa: BLE001 - default to abort
                proceed = False

        if not proceed:
            raise SystemExit("Aborted by user (no data and backfill not confirmed)")

        start_dt = datetime.fromisoformat("2023-01-01").replace(tzinfo=timezone.utc)
        end_dt = datetime.now(timezone.utc)
        return start_dt, end_dt

    if max_ts.tzinfo is None:
        max_ts = max_ts.replace(tzinfo=timezone.utc)
    else:
        max_ts = max_ts.astimezone(timezone.utc)

    start_dt = max_ts + timedelta(seconds=1)
    end_dt = datetime.now(timezone.utc)
    return start_dt, end_dt


def main():
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    args = parse_args()

    spark = build("ingest_open_meteo_bronze")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")

    try:
        ensure_tables(spark)
        start_dt, end_dt = resolve_run_window(args, spark)

        if start_dt > end_dt:
            raise SystemExit("start date must be <= end date")

        client = new_client()
        run_id = str(uuid.uuid4())

        # Load locations from the provided JSON config. To limit which locations
        # are ingested, update `configs/locations.json` (keep a minimal set for smaller runs).
        locations = load_locations(args.locations)

        # Define schema once and reuse for chunk DataFrames
        schema = T.StructType(
            [
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
            ]
        )

        # If replacing a range, delete it once before writing any chunks
        if args.mode == "replace-range":
            maybe_delete_range(
                spark,
                start_dt.astimezone(timezone.utc),
                end_dt.astimezone(timezone.utc),
                [loc["location_id"] for loc in locations] if locations else [],
            )

        total_rows = 0

        def merge_df_into_bronze(spark_session, df, run_tag: str):
            # Create a stable temp view name and MERGE; drop the view after
            temp_view = f"staging_{run_tag}"
            df.createOrReplaceTempView(temp_view)
            merge_sql = f"""
            MERGE INTO hadoop_catalog.aq.bronze.raw_open_meteo_hourly t
            USING {temp_view} s
            ON t.location_id = s.location_id AND t.ts = s.ts
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
            """
            logging.info("Merging %s rows into Bronze (temp view=%s)", df.count(), temp_view)
            spark_session.sql(merge_sql)
            spark_session.catalog.dropTempView(temp_view)

        # Process each location and chunk independently to keep driver memory low
        for loc_idx, loc in enumerate(locations):
            lid, lat, lon = loc["location_id"], loc["lat"], loc["lon"]
            cur = start_dt
            chunk_idx = 0
            while cur <= end_dt:
                nxt = min(cur + timedelta(days=args.chunk_days), end_dt)
                try:
                    resp = fetch_chunk(
                        client,
                        lat,
                        lon,
                        cur.strftime("%Y-%m-%d"),
                        nxt.strftime("%Y-%m-%d"),
                    )
                    chunk_rows = response_to_rows(lid, lat, lon, resp)
                    time.sleep(0.2)  # be gentle with the API
                    if not chunk_rows:
                        logging.info("No rows returned for %s %s -> %s", lid, cur, nxt)
                    else:
                        df_chunk = (
                            spark.createDataFrame(chunk_rows, schema=schema)
                            .withColumn("source", F.lit("open-meteo"))
                            .withColumn("run_id", F.lit(run_id))
                            .withColumn("ingested_at", F.current_timestamp())
                        )

                        # Null-out physically impossible negative numbers
                        sanitise_cols = [
                            "pm2_5",
                            "pm10",
                            "dust",
                            "nitrogen_dioxide",
                            "ozone",
                            "sulphur_dioxide",
                            "carbon_monoxide",
                            "aerosol_optical_depth",
                            "uv_index",
                            "uv_index_clear_sky",
                        ]
                        for c in sanitise_cols:
                            df_chunk = df_chunk.withColumn(c, F.when(F.col(c) < 0, None).otherwise(F.col(c)))

                        df_chunk = df_chunk.dropDuplicates(["location_id", "ts"])

                        run_tag = f"raw_{run_id.replace('-', '_')}_{loc_idx}_{chunk_idx}"
                        merge_df_into_bronze(spark, df_chunk, run_tag)
                        total_rows += len(chunk_rows)
                        chunk_idx += 1
                except Exception as exc:  # noqa: BLE001 - continue other chunks
                    logging.warning("Fetch failed for %s %s -> %s: %s", lid, cur, nxt, exc)
                cur = nxt + timedelta(days=1)

        if total_rows == 0:
            logging.info("No rows fetched from Open-Meteo; nothing to write")
            return

        run_housekeeping(spark)

        spark.sql(
            """
            SELECT location_id,
                   MIN(ts) AS min_ts,
                   MAX(ts) AS max_ts,
                   COUNT(*) AS rows
            FROM hadoop_catalog.aq.bronze.raw_open_meteo_hourly
            GROUP BY location_id
            ORDER BY location_id
            """
        ).show(truncate=False)

        print(f"RUN_ID={run_id}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
