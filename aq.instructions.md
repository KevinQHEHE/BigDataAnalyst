---
applyTo: '**'
---

# AQ Lakehouse - Vietnam Air Quality Data Pipeline Instructions

## Project Context (read me first)

**Goal**: Batch-only data lakehouse for Vietnam Air Quality Analytics using Hadoop + YARN, Spark (PySpark), and Apache Iceberg (Hadoop catalog).

**Runtime (cluster)**: 
- Spark 4.0.1, submit jobs to YARN (not local/standalone)
- Iceberg 1.10.x runtime jar is available to Spark  
- Hadoop catalog (HDFS): `hdfs://khoa-master:9000/warehouse/iceberg`
- Default YARN queue: `root.default`

**Bronze source (already ingested)**:
- Catalog: `hadoop_catalog`
- DBs: `aq` (analytics), `ref` (reference)
- Bronze table: `aq.bronze.raw_open_meteo_hourly`
- Columns: `location_id`, `latitude`, `longitude`, `ts`, `aerosol_optical_depth`, `pm2_5`, `pm10`, `dust`, `nitrogen_dioxide`, `ozone`, `sulphur_dioxide`, `carbon_monoxide`, `uv_index`, `uv_index_clear_sky`, `source`, `run_id`, `ingested_at`

**Config & code layout (current)**:
- `/configs/locations.json` - Vietnam cities (Hà Nội, TP. Hồ Chí Minh, Đà Nẵng)
- `/src/aq_lakehouse/spark_session.py` - Spark session builder
- `/jobs/ingest/open_meteo_bronze.py` - Bronze layer ingestion from Open-Meteo API
- `/scripts/submit_yarn.sh` - YARN job submission script
- `/notebooks/test_data.ipynb` - Data exploration notebook

## Hard Rules (must follow)

**Always target distributed mode**:
- Default to YARN. Do not set `.master("local[*]")` or use standalone in production code
- If you must include a local example for tests, keep it isolated under `tests/` or as a commented snippet, never the default path

**Use the existing Spark/Iceberg wiring**:
- Assume the runtime provides Iceberg jars; don't add new ones or switch to Delta/LakeFS/etc
- Use the Hadoop catalog with:
  ```python
  spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog
  spark.sql.catalog.hadoop_catalog.type=hadoop
  spark.sql.catalog.hadoop_catalog.warehouse=hdfs://khoa-master:9000/warehouse/iceberg
  spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  ```

**Don't create junk files or alternate variants**:
- If `ingest.py` exists, don't propose `ingest_enhance.py` or `ingest_simple.py`. Modify in place or add clearly-scoped modules inside `src/aq_lakehouse/`
- No icons/emojis in code or comments. Keep comments technical and concise

**Be compatible with**:
- Python 3.10 & Spark 4.0.1
- Prefer Spark SQL / functions over Pandas/Arrow paths unless required
- If using Arrow/Pandas UDFs, ensure version-safe code paths and fallbacks (avoid hard dependency in core ETL)

## Naming & Table Placement (Iceberg)

**Bronze (raw)**: `hadoop_catalog.aq.bronze.raw_open_meteo_hourly` ✓ (exists)

**Silver (cleaned & conformed)**: `hadoop_catalog.aq.s_clean_hourly`

**Gold (BI-ready / marts)**:
- Facts: `hadoop_catalog.aq.f_hourly_air_quality`  
- Dims: `hadoop_catalog.ref.d_location`, `hadoop_catalog.ref.d_time`

Use lowercase `snake_case` for columns; avoid spaces; include `ts` as event time.

## IO & Partitions

**When writing Iceberg**: 
- Use `mode("append")` or Iceberg's MERGE/UPSERT patterns; don't overwrite without a WHERE clause
- Prefer `hour` or `date` partition transforms where helpful (e.g., `partitioned by (days(ts))`)—but do not change Bronze layout retroactively

**Performance defaults (safe)**:
- Let submit-time configs set executors; do not hardcode executor counts in code  
- Avoid collecting big DataFrames to the driver. Use `show(20, truncate=False)` for samples

**Config-driven code**:
- Read locations from `/configs/locations.json`. No hardcoded coordinates in job code

**Repro & idempotency**:
- Bronze ingestion must be deduplicated by `(location_id, ts)` when rerun. Use Iceberg MERGE INTO or write-then-dedupe in Silver

## How to Build Spark Sessions (templates)

**Production job (preferred – rely on spark-submit --master yarn)**:

```python
from pyspark.sql import SparkSession
import os

WAREHOUSE_URI = os.getenv("WAREHOUSE_URI", "hdfs://khoa-master:9000/warehouse/iceberg")

def build(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # Do NOT set .master("local")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog") 
        .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
        .config("spark.sql.catalog.hadoop_catalog.warehouse", WAREHOUSE_URI)
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
```

**Local-only test snippet (keep inside tests/ or comments)**:
```python
# TESTS ONLY:
# spark = (SparkSession.builder.master("local[2]").appName("unit-test").getOrCreate())
```

## Python & PySpark Coding Standards

**Style & tooling**: PEP8, type hints where practical, docstrings for public functions

**Project config**: prefer `requirements.txt` (current setup), black/isort/flake8 optional

**Imports**: standard lib → third-party → local; no wildcard imports

**Logging**: use `logging` lib; no print in production jobs (except brief CLI messages)

**DataFrame rules**:
- Schema first: define explicit schemas for Bronze ingest; avoid `inferSchema` for core paths
- Use `pyspark.sql.functions` (e.g., `from pyspark.sql import functions as F`) and SQL with temporary views
- Avoid unnecessary `.toPandas()`; when sampling, limit rows

**Testing**: small, deterministic samples under `tests/` with local master; never hit HDFS in unit tests unless marked `@slow`

## Lakehouse Layer Contracts (inputs/outputs)

**Bronze → Silver**:
- Input: `aq.bronze.raw_open_meteo_hourly` (raw hourly metrics from Open-Meteo API)
- Output: `aq.s_clean_hourly` with standardized units, deduped `(location_id, ts)`, cleaned nulls, and source lineage columns `ingested_at`, `run_id`

**Silver → Gold**:
- Dims: `ref.d_location` (id, name, lat, lon), `ref.d_time` (date_key, hour, day, month, year, dow)
- Fact: `aq.f_hourly_air_quality` (surrogate keys to dims + measures, e.g., `pm2_5`, `pm10`, `ozone`, etc.)

**Write semantics**: always append + upsert safely; never drop/overwrite tables silently

## Job Submission & Runtime

**Use existing submit script**:
```bash
# Example: Ingest data for date range
bash scripts/submit_yarn.sh ingest/open_meteo_bronze.py \
    --locations configs/locations.json \
    --start 2024-01-01 \
    --end 2024-01-31 \
    --chunk-days 10

# Example: Update from database (incremental)  
bash scripts/submit_yarn.sh ingest/open_meteo_bronze.py \
    --locations configs/locations.json \
    --update-from-db
```

**Environment variables**:
- `WAREHOUSE_URI`: defaults to `hdfs://khoa-master:9000/warehouse/iceberg`
- `PYTHONPATH`: automatically set to include `src/` by submit script

## Vietnam-Specific Context

**Locations** (defined in `configs/locations.json`):
- Hà Nội: 21.028511°N, 105.804817°E
- TP. Hồ Chí Minh: 10.762622°N, 106.660172°E  
- Đà Nẵng: 16.054406°N, 108.202167°E

**Timezone**: All timestamps stored in UTC, partitioned by `days(ts)`

**Data source**: Open-Meteo Air Quality API (CAMS Global domain)

## PR / Review Guidance for Copilot

When Copilot proposes changes or reviews PRs, it should:

**Check distributed compliance**: no `.master("local")` in non-test code; no standalone assumptions

**Respect existing files**: modify existing modules; avoid file proliferation

**Validate schemas**: ensure column names/types match contracts; migration scripts for breaking changes

**Edge cases**: handle missing coordinates, empty API windows, duplicate `(location_id, ts)`

**Performance**: avoid `collect()`/`count()` on large tables; use `limit` + `explain()`

**Security**: no secrets in code; configs loaded from env or files under `/configs/`

## Copilot Chat Usage Tips (for this repo)

**Prefer generating Spark SQL / PySpark examples that run on YARN**

**If asked for ingestion/transforms**: use Iceberg DataFrameWriter (`format("iceberg")` or `table(...)`) and SQL MERGE for upserts

**For notebooks (analysis)**: default to matplotlib (no seaborn; no icons/emojis)

**For docs/answers**: keep them actionable and cluster-correct (YARN, Hadoop catalog, HDFS paths)

## What NOT to do

❌ Switch to local/standalone Spark in production code

❌ Introduce new storage layers (Delta, Hudi) or new catalogs  

❌ Create parallel file variants (`*_v2.py`, `*_simple.py`)

❌ Use emojis/icons anywhere in code or docs generated by Copilot

❌ Rely on Pandas/Arrow for core ETL paths

❌ Hardcode Vietnamese city coordinates (use `configs/locations.json`)