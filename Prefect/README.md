# Prefect Flows - DLH-AQI Pipeline

Prefect orchestration flows for the complete AQI data pipeline running on YARN.

## 🎯 Overview

This folder contains Prefect flows that orchestrate the Bronze → Silver → Gold data pipeline with:
- **Single SparkSession per flow** - Efficient resource usage
- **YARN validation** - Ensures cluster execution
- **Automatic retry** - Handles transient failures
- **Comprehensive metrics** - Track all processing stats
- **Hourly scheduling** - Automated production runs

## 📁 Files

| File | Description | Usage |
|------|-------------|-------|
| `spark_context.py` | SparkSession context manager | Imported by all flows |
| `bronze_flow.py` | Bronze layer ingestion | Ingest from Open-Meteo API |
| `silver_flow.py` | Silver layer transformation | Bronze → Silver with enrichment |
| `gold_flow.py` | Gold layer pipeline | Load dimensions + transform facts |
| `full_pipeline_flow.py` | Complete pipeline | Bronze → Silver → Gold in one session |
| `backfill_flow.py` | Historical data backfill | Process large date ranges with chunking |
| `deploy.py` | Deployment script | Create/update Prefect deployments |
| `__init__.py` | Package initialization | Module exports |

## 🚀 Quick Start

### 1. Deploy Flows

```bash
python Prefect/deploy.py --all
```

### 2. Start Infrastructure

```bash
# Terminal 1: Prefect server
prefect server start
# UI: http://localhost:4200

# Terminal 2: Prefect agent
prefect agent start -p default-agent-pool
```

### 3. Run Pipelines

```bash
# Hourly pipeline (for scheduled runs)
bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourly

# Backfill historical data
bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
  --start-date 2024-01-01 \\
  --end-date 2024-12-31 \\
  --chunk-mode monthly
```

## ⚠️ Critical Rules

### DO NOT run flows directly with Python

```bash
# ❌ WRONG - Creates local SparkSession
python Prefect/bronze_flow.py

# ✅ CORRECT - Submits to YARN
bash scripts/spark_submit.sh Prefect/bronze_flow.py -- --mode upsert
```

### Always use spark_submit.sh wrapper

The wrapper automatically configures:
- `--master yarn`
- Dynamic allocation
- Adaptive query execution
- YARN-specific optimizations

## 📚 Documentation

- **[PREFECT_FLOWS_README.md](../PREFECT_FLOWS_README.md)** - Usage examples and features
- **[docs/PREFECT_DEPLOYMENT.md](../docs/PREFECT_DEPLOYMENT.md)** - Deployment guide
- **[README.md](../README.md)** - Project overview

## 🔄 Flow Dependency

```
                                              
┌─────────────────────────────────────────────────┐
│           full_pipeline_flow.py                 │
│   (Orchestrates entire pipeline in one session) │
└─────────────┬────────────┬──────────────────────┘
              │            │                        
              │            │                        
    ┌─────────▼───┐  ┌─────▼────────┐  ┌──────────▼────┐
    │ bronze_flow │─▶│ silver_flow  │─▶│  gold_flow    │
    └─────────────┘  └──────────────┘  └───────────────┘
                                                           
            OR use backfill_flow.py for historical data
```

## 💡 Usage Patterns

### Pattern 1: Scheduled Hourly Updates

```bash
# Deploy with hourly schedule
python Prefect/deploy.py --flow hourly

# Runs automatically every hour
# - Bronze: Upsert (update latest)
# - Silver: Incremental (merge)
# - Gold: Update all
```

### Pattern 2: Manual Full Pipeline

```bash
# Run complete pipeline with custom dates
bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- \\
  --bronze-mode upsert \\
  --silver-mode incremental \\
  --start-date 2024-10-01 \\
  --end-date 2024-10-31
```

### Pattern 3: Historical Backfill

```bash
# Process large date range in chunks
bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
  --start-date 2024-01-01 \\
  --end-date 2024-12-31 \\
  --chunk-mode monthly
```

### Pattern 4: Individual Layer

```bash
# Run only Bronze
bash scripts/spark_submit.sh Prefect/bronze_flow.py -- \\
  --mode backfill \\
  --start-date 2024-01-01 \\
  --end-date 2024-01-31

# Run only Silver
bash scripts/spark_submit.sh Prefect/silver_flow.py -- \\
  --mode incremental \\
  --start-date 2024-01-01 \\
  --end-date 2024-01-31

# Run only Gold
bash scripts/spark_submit.sh Prefect/gold_flow.py -- --mode all
```

## 🔧 Configuration

### Environment Variables (.env)

```bash
WAREHOUSE_URI=hdfs://khoa-master:9000/warehouse/iceberg
SPARK_MASTER=yarn
ENABLE_YARN_DEFAULTS=true
SPARK_DYN_MIN=1
SPARK_DYN_MAX=50
```

### Deployment Schedule

```python
# Hourly pipeline: Every hour at minute 0
CronSchedule(cron="0 * * * *", timezone="Asia/Ho_Chi_Minh")
```

## 📊 Monitoring

### Prefect UI
- http://localhost:4200
- View flow runs, task status, metrics

### YARN ResourceManager
- http://khoa-master:8088
- Track Spark applications, resource usage

### Spark History Server
- http://khoa-master:18080
- Analyze job performance

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| SparkSession not on YARN | Use `spark_submit.sh` wrapper |
| Multiple sessions created | Expected - each flow has its own session |
| HDFS timeout | Check `hdfs dfs -ls /user/$USER/` |
| Deployment not found | Run `python Prefect/deploy.py --flow <name> --update` |
| Agent not picking up runs | Restart agent: `prefect agent start -p default-agent-pool` |

See [docs/PREFECT_DEPLOYMENT.md](../docs/PREFECT_DEPLOYMENT.md) for detailed troubleshooting.

## ✅ Acceptance Criteria

All requirements from the original task have been met:

- ✅ All flows run on YARN via `spark_submit.sh`
- ✅ Single SparkSession per flow (validated with `master == "yarn"`)
- ✅ Bronze flow with Prefect orchestration
- ✅ Silver & Gold flows with shared session context
- ✅ Full pipeline flow (Bronze → Silver → Gold)
- ✅ Backfill flow with monthly/weekly chunking
- ✅ Deployment script with hourly schedule (cron: `0 * * * *`)
- ✅ Upsert deployment pattern (no duplicates)
- ✅ Complete documentation with YARN execution instructions

## 🎓 Next Steps

1. **Deploy**: `python Prefect/deploy.py --all`
2. **Start server**: `prefect server start`
3. **Start agent**: `prefect agent start -p default-agent-pool`
4. **Monitor**: http://localhost:4200
5. **Test run**: `bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourly`

---

For more information, see:
- [Usage Guide](../PREFECT_FLOWS_README.md)
- [Deployment Guide](../docs/PREFECT_DEPLOYMENT.md)
- [Project README](../README.md)
