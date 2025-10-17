# Prefect Workflows - Complete Guide

## ⚠️ IMPORTANT: YARN Execution Required

**DO NOT run flows directly with `python`**. All Prefect flows MUST be submitted via `spark_submit.sh` to run on YARN cluster.

```bash
# ❌ WRONG - Creates local SparkSession
python Prefect/bronze_flow.py

# ✅ CORRECT - Submits to YARN
bash scripts/spark_submit.sh Prefect/bronze_flow.py -- --mode upsert
```

## 🎯 Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run hourly pipeline (Bronze → Silver → Gold)
bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourly

# 3. Run full pipeline with date range
bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- \\
  --silver-mode incremental \\
  --start-date 2024-10-01 \\
  --end-date 2024-10-31

# 4. Run backfill for historical data
bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
  --start-date 2024-01-01 \\
  --end-date 2024-12-31 \\
  --chunk-mode monthly
```

## 📁 Structure

```
Prefect/                              # ✨ Prefect orchestration flows
├── spark_context.py                  # SparkSession context manager
├── bronze_flow.py                    # Bronze ingestion flow
├── silver_flow.py                    # Silver transformation flow
├── gold_flow.py                      # Gold pipeline flow
├── full_pipeline_flow.py             # Complete Bronze → Silver → Gold
├── backfill_flow.py                  # Historical data backfill with chunking
└── deploy.py                         # Deployment script with hourly schedule

jobs/                                 # Core logic (called by Prefect flows)
├── bronze/
│   └── run_bronze_pipeline.py        # Bronze ingestion logic
├── silver/
│   ├── transform_bronze_to_silver.py # Silver transformation logic
│   └── run_silver_pipeline.py        # Legacy orchestrator
└── gold/
    ├── load_dim_*.py                 # Dimension loaders
    ├── transform_fact_*.py           # Fact transformers
    ├── detect_episodes.py            # Episode detection
    └── run_gold_pipeline.py          # Legacy orchestrator
```

## ⚡ Features

### Key Improvements
- ✅ **Single SparkSession per flow** - No multiple session creation
- ✅ **YARN validation** - Ensures `spark.sparkContext.master == "yarn"`
- ✅ **Context manager** - Automatic session lifecycle management
- ✅ **Retry logic** - Automatic retry on transient failures
- ✅ **Chunked backfill** - Process large date ranges in manageable chunks
- ✅ **Hourly scheduling** - Automated cron-based execution
- ✅ **Comprehensive metrics** - Track all processing statistics

### Bronze Flow (`Prefect/bronze_flow.py`)
- ✅ Ingest from Open-Meteo API to Bronze layer
- ✅ Supports backfill and upsert modes
- ✅ Automatic deduplication
- ✅ Rate limiting and retry logic
- ✅ HDFS locations file support

### Silver Flow (`Prefect/silver_flow.py`)
- ✅ Transform Bronze → Silver with date_key/time_key enrichment
- ✅ MERGE INTO (idempotent upsert)
- ✅ Full refresh or incremental modes
- ✅ Built-in data quality validation
- ✅ Configurable date ranges

### Gold Flow (`Prefect/gold_flow.py`)
- ✅ Load 4 dimension tables (location, pollutant, time, date)
- ✅ Transform 3 fact tables (hourly, daily, episodes)
- ✅ Selective execution (dims only, facts only, or custom)
- ✅ Episode detection with configurable thresholds
- ✅ Aggregated metrics

### Full Pipeline Flow (`Prefect/full_pipeline_flow.py`)
- ✅ Complete Bronze → Silver → Gold orchestration
- ✅ Single SparkSession for all stages
- ✅ Skip individual stages
- ✅ Hourly mode for scheduled runs
- ✅ Comprehensive error handling

### Backfill Flow (`Prefect/backfill_flow.py`)
- ✅ Process large historical date ranges
- ✅ Monthly, weekly, or daily chunking
- ✅ Progress tracking per chunk
- ✅ Summary report with failed chunks
- ✅ Resume capability

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

## � Usage Examples

### 1. Bronze Ingestion

```bash
# Upsert mode (default) - update from latest to now
bash scripts/spark_submit.sh Prefect/bronze_flow.py -- --mode upsert

# Backfill historical data
bash scripts/spark_submit.sh Prefect/bronze_flow.py -- \\
  --mode backfill \\
  --start-date 2024-01-01 \\
  --end-date 2024-12-31
```

### 2. Silver Transformation

```bash
# Incremental mode (merge/upsert)
bash scripts/spark_submit.sh Prefect/silver_flow.py -- --mode incremental

# Full refresh (overwrite)
bash scripts/spark_submit.sh Prefect/silver_flow.py -- \\
  --mode full \\
  --start-date 2024-01-01 \\
  --end-date 2024-12-31
```

### 3. Gold Pipeline

```bash
# Load all dimensions and facts
bash scripts/spark_submit.sh Prefect/gold_flow.py -- --mode all

# Load only dimensions
bash scripts/spark_submit.sh Prefect/gold_flow.py -- --mode dims

# Custom selection
bash scripts/spark_submit.sh Prefect/gold_flow.py -- \\
  --mode custom \\
  --dims location,pollutant \\
  --facts hourly,daily
```

### 4. Full Pipeline

```bash
# Hourly pipeline (for scheduled runs)
bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourly

# Custom pipeline with skips
bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- \\
  --bronze-mode upsert \\
  --silver-mode incremental \\
  --skip-validation
```

### 5. Backfill Historical Data

```bash
# Monthly chunks (recommended for large ranges)
bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
  --start-date 2024-01-01 \\
  --end-date 2024-12-31 \\
  --chunk-mode monthly

# Weekly chunks
bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
  --start-date 2024-10-01 \\
  --end-date 2024-10-31 \\
  --chunk-mode weekly

# Skip certain stages
bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
  --start-date 2024-01-01 \\
  --end-date 2024-12-31 \\
  --skip-bronze \\
  --chunk-mode monthly
```

## �📊 Monitoring with Prefect

### Start Prefect server:
```bash
prefect server start
# UI: http://localhost:4200
```

### View real-time metrics:
- Flow execution time per stage
- Task success/failure rates
- Records processed at each layer
- SparkSession validation (YARN master check)
- Error logs and stack traces

## � Deployment & Scheduling

### Deploy flows with hourly schedule:

```bash
# Deploy hourly pipeline (runs every hour)
python Prefect/deploy.py --flow hourly

# Deploy all flows
python Prefect/deploy.py --all

# Update existing deployment
python Prefect/deploy.py --flow hourly --update
```

### Start Prefect agent:

```bash
prefect agent start -p default-agent-pool
```

### Manual flow runs:

```bash
# Run hourly pipeline
prefect deployment run 'hourly-pipeline-flow/aqi-pipeline-hourly'

# Run backfill with parameters
prefect deployment run 'backfill-flow/aqi-pipeline-backfill' \\
  --param start_date=2024-01-01 \\
  --param end_date=2024-12-31 \\
  --param chunk_mode=monthly
```

## 🔧 Configuration

### Environment Variables (.env)

```bash
# Iceberg warehouse
WAREHOUSE_URI=hdfs://khoa-master:9000/warehouse/iceberg

# YARN settings (optional, handled by spark_submit.sh)
SPARK_MASTER=yarn
SPARK_HOME=/opt/spark
ENABLE_YARN_DEFAULTS=true
SPARK_DYN_MIN=1
SPARK_DYN_MAX=50
SPARK_AQE_ENABLED=true
```

### Spark Submit Configuration (scripts/spark_submit.sh)

The wrapper automatically configures:
- `--master yarn`
- `--deploy-mode client`
- Dynamic allocation
- Adaptive query execution
- YARN archive for dependencies

See [docs/PREFECT_DEPLOYMENT.md](docs/PREFECT_DEPLOYMENT.md) for detailed deployment guide.

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
