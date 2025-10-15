# Prefect Workflows - Quick Reference

## 🎯 Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run Silver transformation
python jobs/silver/prefect_silver_flow.py --date-range 2024-10-01 2024-10-31

# 3. Run Gold dimensions
python jobs/gold/prefect_gold_flow.py

# 4. Run full pipeline
python jobs/gold/prefect_gold_flow.py --full-pipeline
```

## 📁 Structure

```
jobs/
├── silver/
│   ├── transform_bronze_to_silver.py  # Core transformation (returns dict)
│   ├── run_silver_pipeline.py         # Legacy orchestrator
│   └── prefect_silver_flow.py         # ✨ NEW: Prefect flow
│
└── gold/
    ├── load_dim_location.py           # Load 3 locations (returns dict)
    ├── load_dim_pollutant.py          # Load 10 pollutants (returns dict)
    ├── load_dim_time.py               # Generate 24 hours (returns dict)
    ├── load_dim_date.py               # Extract dates from silver (returns dict)
    ├── run_gold_pipeline.py           # Legacy orchestrator
    └── prefect_gold_flow.py           # ✨ NEW: Prefect flow
```

## ⚡ Features

### Silver Flow (`prefect_silver_flow.py`)
- ✅ Transform Bronze → Silver with date_key/time_key
- ✅ MERGE INTO (idempotent operations)
- ✅ Automatic retry on failure (2 retries, 60s delay)
- ✅ Built-in validation task
- ✅ Monthly backfill support
- ✅ Configurable date ranges

### Gold Flow (`prefect_gold_flow.py`)
- ✅ Load 4 dimension tables
- ✅ Parallel execution (location + pollutant)
- ✅ Selective loading (--only, --skip)
- ✅ Full pipeline mode (Bronze → Silver → Gold)
- ✅ Aggregated metrics

## 🔄 Return Values

All functions now return structured dictionaries for Prefect monitoring:

```python
# Silver transformation
{
    "status": "success" | "skipped",
    "records_processed": 2232,
    "duration_seconds": 45.2,
    "start_date": "2024-10-01",
    "end_date": "2024-10-31",
    "bronze_table": "hadoop_catalog.lh.bronze.open_meteo_hourly",
    "silver_table": "hadoop_catalog.lh.silver.air_quality_hourly_clean"
}

# Gold dimensions
{
    "status": "success",
    "records_loaded": 3,  # or records_generated for time/date
    "duration_seconds": 12.5,
    "source_file": "hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl",
    "target_table": "hadoop_catalog.lh.gold.dim_location"
}
```

## 📊 Monitoring with Prefect

### Start Prefect server:
```bash
prefect server start
# UI: http://localhost:4200
```

### View real-time metrics:
- Flow execution time
- Task success/failure rates  
- Records processed
- Error logs

## 🚀 Deployment

See [PREFECT_DEPLOYMENT.md](../docs/PREFECT_DEPLOYMENT.md) for:
- Scheduling workflows
- Setting up work pools
- Configuring notifications
- Production best practices

## 🎨 Examples

### Example 1: Process specific month
```bash
python jobs/silver/prefect_silver_flow.py --date-range 2024-10-01 2024-10-31
```

### Example 2: Backfill multiple months
```bash
python jobs/silver/prefect_silver_flow.py --backfill-monthly --date-range 2024-01-01 2024-12-31
```

### Example 3: Reload specific dimensions
```bash
python jobs/gold/prefect_gold_flow.py --only location pollutant
```

### Example 4: Full pipeline
```bash
python jobs/gold/prefect_gold_flow.py --full-pipeline --date-range 2024-10-01 2024-10-31
```

## 🔗 Legacy vs Prefect

| Feature | Legacy Scripts | Prefect Flows |
|---------|---------------|---------------|
| Orchestration | Manual | Automatic |
| Retry Logic | ❌ No | ✅ Yes (configurable) |
| Monitoring | Logs only | UI Dashboard + Logs |
| Parallel Execution | ❌ No | ✅ Yes |
| Scheduling | Cron | Built-in scheduler |
| Error Handling | Basic | Advanced with retries |
| Metrics | Print to console | Structured returns |

**Recommendation:** Use Prefect flows for production, keep legacy scripts for manual debugging.

## 📝 Notes

1. **Idempotency**: All operations are idempotent - safe to re-run
2. **Dependencies**: Gold date dimension requires silver data
3. **Parallelism**: Location and pollutant load in parallel
4. **Validation**: Silver flow includes optional validation step
5. **YARN**: All Spark jobs run on YARN cluster via `spark_submit.sh`
