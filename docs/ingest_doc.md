# Air Quality Lakehouse – Bronze Ingestion Playbook

## 1. Concept & Architecture

### Goal
Ingest hourly air-quality observations from the Open-Meteo CAMS API into an Apache Iceberg Bronze table and preserve data quality through optional cleanup routines.

### Key Features
- **Source**: CAMS global model pollutants (`pm2_5`, `pm10`, `ozone`, `nitrogen_dioxide`, `sulphur_dioxide`, `carbon_monoxide`, etc.).
- **Granularity**: Hourly records for a curated set of locations listed in `configs/locations.json` (or supplied via `LOCATIONS_CONFIG`).
- **Bronze Table**: `hadoop_catalog.aq.raw_open_meteo_hourly`, partitioned by `days(ts)` and retaining raw-normalized measurements.
- **Ingestion Semantics**: Iceberg `MERGE` (insert-only) keeps `(location_id, ts)` unique so reruns over overlapping windows remain idempotent.
- **Cleanup Job**: Optional Spark task sanitizes negative readings, deduplicates keys, and rewrites only affected partitions.

### Data Flow
```
Open-Meteo API → Ingest Job (PySpark) → Bronze Table (Iceberg)
           ↘                                ↘
            └──────── Cleanup Job ─────────→  Verified Bronze
```

## 2. Bronze Reset (Optional)

To start testing from an empty Bronze table while keeping the schema intact:

```sql
TRUNCATE TABLE hadoop_catalog.aq.raw_open_meteo_hourly;
-- or
DELETE FROM hadoop_catalog.aq.raw_open_meteo_hourly WHERE TRUE;
```

## 3. Ingestion Job (`jobs/ingest/open_meteo_bronze.py`)

### Primary Command (MERGE insert-only)

Fetch data between 2022-01-01 and 2025-09-01, chunk API calls into 10-day windows, and upsert missing rows only:

```bash
bash script/submit_yarn.sh jobs/ingest/open_meteo_bronze.py --start-date 2022-01-01 --end-date   2025-09-01 --chunk-days 10 --mode merge --fetch-mode missing --locations configs/locations.json
```

Re-running the same range (or a subset such as `2023-01-01` → `2025-01-01`) keeps the table clean because the Iceberg `MERGE` matches on `(location_id, ts)` and skips duplicates.

### Force Full Refetch

```bash
bash script/submit_yarn.sh jobs/ingest/open_meteo_bronze.py --start-date 2022-01-01 --end-date   2025-09-01 --chunk-days 10 --mode merge --fetch-mode all
```

### Append Without Upsert (not recommended except for troubleshooting)

```bash
bash script/submit_yarn.sh jobs/ingest/open_meteo_bronze.py --start-date 2022-01-01 --end-date   2025-09-01 --chunk-days 10 --mode append
```

### Options Summary

- `--chunk-days`: Maximum inclusive span per API request (default `10`).
- `--fetch-mode {missing, all}`: `missing` only pulls uncovered dates; `all` re-fetches the entire window.
- `--mode {merge, append}`: `merge` performs the insert-only upsert; `append` bypasses MERGE.
- `--locations`: Coordinate configuration (defaults to `configs/locations.json`).
- `--sleep-sec`: Courtesy delay between API calls; default `0.2` seconds.

## 4. Post-Ingest Checks

Run the following SQL to confirm coverage and uniqueness:

### Launch Spark SQL CLI

```bash
export WAREHOUSE_URI=${WAREHOUSE_URI:-hdfs://khoa-master:9000/warehouse/iceberg_test}

$SPARK_HOME/bin/spark-sql \
  --master yarn \
  --deploy-mode client \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop \
  --conf spark.sql.catalog.hadoop_catalog.io-impl=org.apache.iceberg.hadoop.HadoopFileIO \
  --conf spark.sql.catalog.hadoop_catalog.warehouse=$WAREHOUSE_URI \
  --conf spark.sql.catalogImplementation=in-memory \
  --conf spark.sql.warehouse.dir=$WAREHOUSE_URI \
  --conf javax.jdo.option.ConnectionURL=jdbc:derby:;databaseName=/tmp/spark_sql_metastore;create=true
```

Inside the shell run:

```sql
USE CATALOG hadoop_catalog;
SHOW NAMESPACES;  -- expect 'aq'
USE aq;
SHOW TABLES;      -- verify 'raw_open_meteo_hourly'
```

### Coverage Overview

```sql
SELECT location_id,
       MIN(ts) AS min_ts,
       MAX(ts) AS max_ts,
       COUNT(*) AS rows
FROM hadoop_catalog.aq.raw_open_meteo_hourly
GROUP BY location_id
ORDER BY location_id;
```

### Duplicate Keys (should return 0 rows)

```sql
SELECT location_id, ts, COUNT(*) AS c
FROM hadoop_catalog.aq.raw_open_meteo_hourly
GROUP BY location_id, ts
HAVING c > 1
LIMIT 10;
```

### Inspect Dates Present (example test window)

```sql
SELECT DISTINCT DATE(ts) AS d
FROM hadoop_catalog.aq.raw_open_meteo_hourly
WHERE ts >= '2023-01-01' AND ts < '2025-01-02'
ORDER BY d
LIMIT 20;
```

## 5. Cleanup Job (`jobs/ingest/cleanup_bronze.py`)

### Purpose
- Convert negative pollutant values to `NULL`.
- Deduplicate `(location_id, ts)` by keeping the most recent `ingest_ts`.
- Scope work by partition day and rewrite only those partitions via Iceberg `overwritePartitions()`.

### Commands

Dry-run to review statistics before writing:

```bash
bash script/submit_yarn.sh jobs/ingest/cleanup_bronze.py --start-date 2023-01-01 --end-date   2025-01-01 --dry-run
```

Commit the cleanup to the matching day partitions:

```bash
bash script/submit_yarn.sh jobs/ingest/cleanup_bronze.py --start-date 2023-01-01 --end-date   2025-01-01
```

Limit to one location if needed:

```bash
bash script/submit_yarn.sh jobs/ingest/cleanup_bronze.py --start-date 2024-01-01 --end-date   2024-12-31 --location-id vn_hn_hoankiem --dry-run
```

### Post-Cleanup Verification

```sql
SELECT location_id, ts, COUNT(*) AS c
FROM hadoop_catalog.aq.raw_open_meteo_hourly
WHERE DATE(ts) BETWEEN '2023-01-01' AND '2025-01-01'
GROUP BY location_id, ts
HAVING c > 1
LIMIT 10;
```

## 6. Core Concepts to Remember

- Bronze stores raw-normalized hourly observations in Iceberg partitions `days(ts)`.
- Ingestion defaults to `--mode merge`, which issues an Iceberg `MERGE INTO`:
  - `WHEN NOT MATCHED THEN INSERT` so new rows land in Bronze.
  - `WHEN MATCHED` rows are ignored (insert-only), guaranteeing idempotence for overlapping windows.
- Each batch removes duplicates before writing with `dropDuplicates(['location_id','ts'])`, avoiding chunk overlap artifacts.
- Cleanup is optional but recommended on a cadence. It sanitizes negatives, deduplicates by `ingest_ts`, and rewrites only impacted partitions.
- Validation queries (coverage, duplicates, date presence) help confirm ingest and cleanup results quickly.

## 7. Maintenance Tips

- Run ingestion daily or on demand with `--fetch-mode missing` to minimize API calls.
- Trigger cleanup after large backfills or weekly to keep Bronze tidy.
- Schedule Iceberg maintenance (data file rewrite, snapshot expiry) monthly to maintain healthy file sizes and metadata.
