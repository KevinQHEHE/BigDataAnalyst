# Bronze → Silver → Gold Pipeline Guide

This guide explains how the hourly Open-Meteo ingestion job, Iceberg Bronze table, Silver view, and Gold aggregation table fit together. The goal is to offer a single command that is safe to re-run, supports range replacement, and keeps downstream layers in sync automatically.

## 1. One-command pipeline

Use `scripts/run_pipeline.sh` to orchestrate the complete flow:

```bash
START=2024-01-01 END=2024-01-07 ./scripts/run_pipeline.sh
```

The script performs five stages:

1. Submit the Bronze ingest job to YARN via `spark-submit`, capture the emitted `RUN_ID`, and write data via Iceberg `MERGE` so the run is idempotent.
2. Upsert the `dim_locations` table and refresh the Silver view (`v_silver_air_quality_hourly`).
3. Refresh the Gold table:
   - Incremental mode (`FULL=false`): compute impacted `(location_id, date)` pairs from the captured `RUN_ID` and merge them.
   - Full rebuild (`FULL=true`): truncate and repopulate the Gold table from Silver.
4. Run data-quality checks (null timestamps, duplicate keys, pollutant range checks). The script exits with a non-zero status if any check fails.
5. Housekeep Gold Iceberg metadata (compaction + optional snapshot expiration) and delete Spark staging artefacts on HDFS/local disk.

Important environment variables:

- `START` / `END`: inclusive UTC date boundaries (default `2024-01-01` → `2025-09-20`).
- `MODE`: Bronze write mode (`upsert` or `replace-range`, default `upsert`).
- `FULL`: `true` triggers a Gold rebuild, otherwise incremental.
- `LOCATION_IDS`: optional space-delimited list to restrict ingestion/deletion to specific `location_id` values.
- `LOCATIONS_FILE`: override path to `configs/locations.json`.
- `CHUNK_DAYS`: adjust API chunk size (default `10`).
- `EXPIRE_SNAPSHOTS`: set to `true` to enable Iceberg snapshot expiration in the pipeline (defaults to `false` to avoid long-running procedures on constrained clusters).

All Spark interactions are routed to YARN with the Iceberg catalog configured for `hdfs://khoa-master:9000/warehouse/iceberg`. Arrow execution is disabled by default to avoid Python/Arrow compatibility issues and the Hive catalog is forced to `in-memory` to prevent local Derby locks on shared hosts.

The dimension seed set is merged with this form (note the `UNION ALL` pattern to avoid column aliases in `MERGE`):

```sql
MERGE INTO hadoop_catalog.aq.dim_locations d
USING (
  SELECT 'Hà Nội' AS location_id, 21.028511 AS latitude, 105.804817 AS longitude
  UNION ALL
  SELECT 'TP. Hồ Chí Minh', 10.762622, 106.660172
  UNION ALL
  SELECT 'Đà Nẵng', 16.054406, 108.202167
) v
ON d.location_id = v.location_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

## 2. Bronze ingest job (`jobs/ingest/open_meteo_bronze.py`)

### Table contract

- Namespace/table: `hadoop_catalog.aq.raw_open_meteo_hourly`.
- Logical key: `(location_id, ts)`.
- Columns include pollutant metrics, lat/lon, source (`open-meteo`), `run_id`, and `ingested_at`.
- Table properties set to Iceberg format v2 with 128 MB target file size and hash distribution.

### CLI usage

Always launch the job through Spark/YARN so that distributed resources, HDFS, and Iceberg integrations are honoured:

```bash
bash script/submit_yarn.sh jobs/ingest/open_meteo_bronze.py \
  --locations configs/locations.json \
  --start 2024-01-01 \
  --end   2024-01-07 \
  --mode upsert
```

> Tip: running the module with `python3 ...` bypasses Spark/YARN and will fail when the Spark session tries to reach the cluster master. Always use `spark-submit` (or the provided wrapper) in shared environments.

If you previously ran the script locally, the pipeline will automatically rename the legacy `ingest_ts` column to `ingested_at` the next time it runs to keep the schema consistent.

Notable flags:

- `--mode upsert`: default behaviour, performs `MERGE` without prior deletes.
- `--mode replace-range`: deletes existing rows between `--start` and `--end` for the requested `location_id` values before merging.
- `--location-id <id>`: repeatable; restricts fetch/deletes to a subset of locations.
- `--update-from-db`: derive `start`/`end` from the current max timestamp in Bronze. If the table is empty, the script prompts for a backfill from `2023-01-01` (use `--yes` for non-interactive runs).
- `--chunk-days`: split API calls to respect payload limits.

### Runtime behaviour

- Fetches hourly measurements from Open-Meteo using a cached, retried HTTP client.
- Normalises negative pollutant readings to `NULL` in Bronze while keeping other values raw.
- Records a unique `run_id` (UUID) and prints `RUN_ID=<value>` to stdout for callers to consume.
- Executes Iceberg maintenance procedures after each run:
  - `CALL system.rewrite_data_files`
  - `CALL system.expire_snapshots(..., -30 days)`
  - `CALL system.remove_orphan_files`

## 3. Silver view (`hadoop_catalog.aq.v_silver_air_quality_hourly`)

Silver is a view; no data movement is required after Bronze updates. The view applies uniform naming, null handling, and joins coordinates:

```sql
CREATE NAMESPACE IF NOT EXISTS spark_catalog.aq;

DROP VIEW IF EXISTS spark_catalog.aq.v_silver_air_quality_hourly;

CREATE VIEW spark_catalog.aq.v_silver_air_quality_hourly AS
SELECT
  r.location_id,
  CAST(r.ts AS TIMESTAMP) AS ts_utc,
  CAST(to_date(r.ts) AS DATE) AS date,
  NULLIF(r.aerosol_optical_depth, CASE WHEN r.aerosol_optical_depth < 0 THEN r.aerosol_optical_depth END) AS aod,
  NULLIF(r.pm2_5, CASE WHEN r.pm2_5 < 0 THEN r.pm2_5 END) AS pm25,
  NULLIF(r.pm10, CASE WHEN r.pm10 < 0 THEN r.pm10 END) AS pm10,
  NULLIF(r.nitrogen_dioxide, CASE WHEN r.nitrogen_dioxide < 0 THEN r.nitrogen_dioxide END) AS no2,
  NULLIF(r.ozone, CASE WHEN r.ozone < 0 THEN r.ozone END) AS o3,
  NULLIF(r.sulphur_dioxide, CASE WHEN r.sulphur_dioxide < 0 THEN r.sulphur_dioxide END) AS so2,
  NULLIF(r.carbon_monoxide, CASE WHEN r.carbon_monoxide < 0 THEN r.carbon_monoxide END) AS co,
  NULLIF(r.uv_index, CASE WHEN r.uv_index < 0 THEN r.uv_index END) AS uv_index,
  NULLIF(r.uv_index_clear_sky, CASE WHEN r.uv_index_clear_sky < 0 THEN r.uv_index_clear_sky END) AS uv_index_clear_sky,
  d.latitude AS lat,
  d.longitude AS lon,
  r.run_id,
  r.ingested_at
FROM hadoop_catalog.aq.raw_open_meteo_hourly r
LEFT JOIN hadoop_catalog.aq.dim_locations d USING (location_id);
```

Any Bronze update is instantly visible through the view, removing the need for refresh jobs.

## 4. Gold table (`hadoop_catalog.aq.gold_air_quality_daily`)

- Iceberg table, partitioned by `(date, location_id)`.
- Columns store daily averages for major pollutants and UV metrics.
- Incremental refresh uses the Bronze `run_id` to isolate affected days/locations before merging.
- Full rebuild truncates and recomputes averages from the Silver view (use when Bronze is fully reingested).

## 5. Data quality expectations

The pipeline validates:

- `ts_utc` must be non-null.
- `(location_id, ts_utc)` pairs must be unique in Silver.
- `pm25` values must lie in `[0, 2000]` (tunable example check).

Failures terminate the script with an error so orchestrators can alert and retry after intervention.

## 6. Manual SQL toolbox

Run `spark-sql` with the same Iceberg catalog configuration to perform ad-hoc checks.

List history and snapshots:

```sql
SELECT * FROM hadoop_catalog.aq.raw_open_meteo_hourly.snapshots ORDER BY committed_at DESC;
```

Inspect Bronze range by location:

```sql
SELECT location_id, MIN(ts) AS min_ts, MAX(ts) AS max_ts, COUNT(*) AS rows
FROM hadoop_catalog.aq.raw_open_meteo_hourly
GROUP BY location_id
ORDER BY location_id;
```

Delete a specific date range manually (if not using `--mode replace-range`):

```sql
DELETE FROM hadoop_catalog.aq.raw_open_meteo_hourly
WHERE location_id IN ('Hà Nội', 'Đà Nẵng')
  AND ts BETWEEN TIMESTAMP '2024-07-01 00:00:00' AND TIMESTAMP '2024-07-07 23:59:59';
```

## 7. Troubleshooting tips

- **RUN_ID missing**: ensure no additional `awk` filters are altering ingest output; the job prints `RUN_ID=...` on completion.
- **Arrow errors**: Python/Arrow mismatches are avoided by pinning `numpy==1.26.4`, `pyarrow==14.0.2` and disabling Arrow (`spark.sql.execution.arrow.pyspark.enabled=false`).
- **Partial deletes**: when replacing a date range, pass `MODE=replace-range` (or `--mode replace-range`) and provide the same `START`/`END` window used for ingestion.
- **Gold drift after manual Bronze edits**: rerun `./scripts/run_pipeline.sh` with `FULL=true` to rebuild the Gold table.
- **Spark staging leftovers**: the pipeline script purges `/user/<user>/.sparkStaging` automatically. If a run is interrupted, you can manually call `hdfs dfs -rm -r -f /user/$USER/.sparkStaging` and rerun the pipeline.

## 8. Full reset / cleanup

To wipe all data layers and cached artefacts before a full rebuild, run:

```bash
./scripts/reset_pipeline_state.sh
```

The script truncates Bronze, Dim, and Gold tables (when present), drops the Silver view, and clears local/HDFS staging artefacts. Snapshot expiration is disabled by default (set `RESET_EXPIRE_SNAPSHOTS=true` to enable it). The NameNode must be out of safe mode (use `hdfs dfsadmin -safemode leave` if necessary); otherwise the script exits early to keep Spark from failing mid-reset. Afterward, execute `./scripts/run_pipeline.sh` to repopulate the stack from scratch.

Keeping Bronze as the single source of truth and deriving Silver via a view keeps the system easy to reason about while guaranteeing that reruns, range replacements, and full rebuilds remain safe and predictable.
