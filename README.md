# AQ Lakehouse — Vietnam Air Quality Pipeline

A modular lakehouse pipeline that ingests hourly air-quality measurements from the Open-Meteo API, stores them in an Iceberg-backed Bronze table, derives a Silver view for clean analytics, and maintains a daily Gold table for BI dashboards. The flow is idempotent, supports range replacement, and can rebuild downstream layers automatically.

## Architecture at a glance

- **Bronze (`hadoop_catalog.aq.raw_open_meteo_hourly`)** – Source-of-truth Iceberg table keyed by `(location_id, ts)`. Data is written via `MERGE` with per-run UUIDs and timestamps for lineage.
- **Dim (`hadoop_catalog.aq.dim_locations`)** – Type 1 dimension carrying the authoritative latitude/longitude for each location.
- **Silver (`spark_catalog.aq.v_silver_air_quality_hourly`)** – A view that cleans negative readings, keeps timestamps in UTC, and enriches with coordinates (backed by the session catalog while sourcing data from Iceberg Bronze).
- **Gold (`hadoop_catalog.aq.gold_air_quality_daily`)** – Iceberg table storing per-day pollutant averages. Only affected `(location_id, date)` pairs are updated per run.
- **Orchestration (`scripts/run_pipeline.sh`)** – Single command to run ingest → housekeeping → dimension/view refresh → incremental/full Gold update → data-quality checks → staging cleanup.
- **Reset (`scripts/reset_pipeline_state.sh`)** – Optional utility to truncate/drop downstream artefacts and clear staging/cache files before a fresh rebuild.

## Quick start

1. Install dependencies (Python 3.10+ recommended):
   ```bash
   pip install -r requirements.txt
   ```
2. Make sure Spark and Hadoop clients can access `hdfs://khoa-master:9000/` and Iceberg extensions are available on the cluster.
3. Run the full pipeline (submits Spark jobs to YARN):
   ```bash
   START=2024-01-01 END=2024-01-31 ./scripts/run_pipeline.sh
   ```
4. For a Gold rebuild after reloading Bronze entirely:
   ```bash
   START=2023-01-01 END=2025-12-31 FULL=true ./scripts/run_pipeline.sh
   ```

### Environment knobs

- `START` / `END` — inclusive UTC dates for ingestion.
- `MODE` — `upsert` (default) or `replace-range` (pre-delete range before merge).
- `LOCATION_IDS` — optional space-separated list to ingest/delete only those locations.
- `FULL` — `true` triggers a Gold truncate + rebuild from Silver.
- `LOCATIONS_FILE` — override `configs/locations.json` if you maintain multiple location sets.
- `CHUNK_DAYS` — adjust API chunking window (default `10`).

The script captures the `RUN_ID` emitted by the ingest job and passes it to the Gold merge so only impacted partitions are recomputed.

## Ingestion job details

`jobs/ingest/open_meteo_bronze.py` handles API access and Bronze writes (always submit via Spark/YARN using `script/submit_yarn.sh`).

- Reads coordinates from `configs/locations.json` (keys are the `location_id`).
- Uses a cached, retried Open-Meteo client with polite pacing (`time.sleep(0.2)` per API call).
- Sanitises negative pollutant readings to `NULL` before writing.
- Drops duplicate `(location_id, ts)` pairs within the fetched batch.
- Writes through Iceberg `MERGE` so reruns update existing rows instead of duplicating them.
- Prints `RUN_ID=<uuid>` on success and automatically runs Iceberg maintenance procedures.

Range replacement is achieved with `--mode replace-range` which issues an Iceberg `DELETE` for the requested window and location set before the merge. For incremental catch-up runs, use `--update-from-db` to start from the next hour after the current `MAX(ts)`.

## Data layers

| Layer | Contract | Partitioning | Notes |
| --- | --- | --- | --- |
| Bronze | hourly metrics, lat/lon, `run_id`, `ingested_at` | `days(ts)` | Format v2, hash distribution, 128 MB target files |
| Silver | view fields: `location_id`, `ts_utc`, `date`, pollutant columns, `lat`, `lon`, `run_id`, `ingested_at` | n/a | exposes Bronze data cleaned and ready for joins |
| Gold | daily averages: `pm25_avg_24h`, `pm10_avg_24h`, `no2_avg_24h`, `o3_avg_24h`, `so2_avg_24h`, `co_avg_24h`, `uv_index_avg`, `uv_index_cs_avg`, `aod_avg` | `(date, location_id)` | Upsert per `RUN_ID`; rebuildable in one pass |

Adding new KPIs (e.g., AQI calculations, rolling windows) follows the same pattern—derive from Silver keyed by date/location and merge only the changed slices.

## Data quality and housekeeping

- The pipeline runs null, duplication, and pollutant range checks on the Silver view after each execution. Any non-zero count aborts the run.
- Bronze maintenance (`rewrite_data_files`, `expire_snapshots`, `remove_orphan_files`) is executed post-ingest to keep storage tidy.
- Gold maintenance and Spark staging cleanup run at the end of every pipeline invocation (purges `/user/<user>/.sparkStaging`, prunes `./spark-warehouse`, and trims stale HTTP caches).
- All Spark entrypoints force `spark.sql.catalogImplementation=in-memory` to avoid local Derby metastore locks when multiple jobs run on the same host.
- Dependencies pin `numpy==1.26.4` and `pyarrow==14.0.2` to avoid known Arrow/PySpark compatibility issues. Arrow execution is disabled by default (`spark.sql.execution.arrow.pyspark.enabled=false`).

## Project layout

```
jobs/ingest/open_meteo_bronze.py   # Bronze ingestion logic (MERGE + housekeeping)
src/aq_lakehouse/spark_session.py  # Spark builder with Iceberg catalog config
script/submit_yarn.sh              # Helper to submit arbitrary PySpark jobs to YARN
scripts/run_pipeline.sh            # One-command pipeline orchestrator (spark-submit + housekeeping)
scripts/reset_pipeline_state.sh    # Truncate/drop all layers and clear staging/cache files
docs/ingest_bronze.md              # Detailed operations guide (this README's companion)
configs/locations.json             # Location IDs and coordinates
```

## Extending the pipeline

1. **Add locations** – update `configs/locations.json`. The next pipeline run will merge new coordinates into `dim_locations` automatically.
2. **Add measures** – include new hourly fields in `HOURLY_VARS` and extend Bronze/Silver/Gold transformations accordingly.
3. **New Gold metrics** – create another aggregation CTE inside the Gold update block or a separate Iceberg table merged by `RUN_ID`.
4. **Scheduling** – wrap `scripts/run_pipeline.sh` inside cron, Airflow, or another orchestrator. Non-zero exits provide clear failure signals. Set `EXPIRE_SNAPSHOTS=true` if you want the run to call Iceberg’s snapshot expiration procedure at the end.
5. **Cold rebuilds** – run `scripts/reset_pipeline_state.sh` before a historical reload to ensure a clean slate (leave HDFS safe mode first if it is active). Snapshot expiration is skipped by default during the reset; enable it via `RESET_EXPIRE_SNAPSHOTS=true` when needed, then start the pipeline with `FULL=true`.

## Troubleshooting

- **Authentication to HDFS fails**: verify Hadoop configs on the driver and ensure the warehouse path is reachable (`hdfs dfs -ls hdfs://khoa-master:9000/warehouse/iceberg`).
- **`RUN_ID` missing in orchestrator logs**: ensure stdout is not suppressed; the string is printed on the final line by the ingest job.
- **`spark-sql` not on PATH**: point `SPARK_SQL` to the desired binary, e.g. `SPARK_SQL=/opt/spark/bin/spark-sql ./scripts/run_pipeline.sh`.
- **Residual data after a failed run**: execute `./scripts/reset_pipeline_state.sh` to truncate tables, drop views, and clear Spark staging directories before retrying. The reset also removes any local Derby metadata remnants.
- **Replaying a specific period**: set `MODE=replace-range` along with the appropriate `START`/`END` and (optionally) `LOCATION_IDS`. The Gold layer will automatically resynchronise the affected days.

For deeper operational steps, consult `docs/ingest_bronze.md`.
