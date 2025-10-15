# Silver Layer Transformation

Transform bronze data to silver by adding dimensional keys (date_key, time_key) and performing MERGE INTO for idempotent updates.

## Quick Start

### Local Testing
```bash
# Full refresh
python3 jobs/silver/run_silver_pipeline.py --mode full

# Incremental (with date range)
python3 jobs/silver/run_silver_pipeline.py --mode incremental --start-date 2024-01-01 --end-date 2024-12-31
```

### YARN Cluster
```bash
# Full refresh
bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- --mode full

# Incremental
bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- \
  --mode incremental \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

## Architecture

- **Input**: `hadoop_catalog.lh.bronze.open_meteo_hourly`
- **Output**: `hadoop_catalog.lh.silver.air_quality_hourly_clean`
- **Operation**: MERGE INTO (upsert on `location_key`, `ts_utc`)
- **Transformations**:
  - Add `date_key` (YYYYMMDD as integer)
  - Add `time_key` (HH00 as integer, e.g., 1400 for 14:00)
  - Preserve all bronze columns

## Prefect Integration

```python
from prefect import flow, task
from jobs.silver.run_silver_pipeline import execute_silver_transformation

@task
def transform_silver_task(start_date: str, end_date: str):
    return execute_silver_transformation(
        mode="incremental",
        start_date=start_date,
        end_date=end_date
    )

@flow
def silver_flow():
    result = transform_silver_task("2024-01-01", "2024-12-31")
    if not result["success"]:
        raise Exception(f"Silver transformation failed: {result.get('error')}")
    return result
```

## Parameters

- `--mode`: `full` (all data) or `incremental` (with date filters)
- `--start-date`: Start date (YYYY-MM-DD) for incremental mode
- `--end-date`: End date (YYYY-MM-DD) for incremental mode
- `--bronze-table`: Source bronze table (default: `hadoop_catalog.lh.bronze.open_meteo_hourly`)
- `--silver-table`: Target silver table (default: `hadoop_catalog.lh.silver.air_quality_hourly_clean`)
- `--warehouse`: Iceberg warehouse URI (default: from env or `hdfs://khoa-master:9000/warehouse/iceberg`)

## Performance Tips

- Use `--mode incremental` with specific date ranges for daily/weekly updates
- Use `--mode full` only for initial load or major data corrections
- MERGE INTO is idempotent: safe to re-run for the same date range
- Enable dynamic allocation on YARN for automatic resource scaling:
  ```bash
  spark-submit --conf spark.dynamicAllocation.enabled=true ...
  ```

## Troubleshooting

**MERGE fails with "table not found"**:
- Ensure silver table exists (run schema creation first)
- Check warehouse URI and catalog configuration

**Slow performance**:
- Use date range filters to limit data volume
- Check Iceberg table partitioning (should be partitioned by `date_utc`)
- Increase executor memory if processing large date ranges

**No data processed**:
- Verify bronze table has data for the specified date range
- Check date format (must be YYYY-MM-DD)
