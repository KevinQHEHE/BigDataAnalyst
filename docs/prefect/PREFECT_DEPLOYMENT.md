# Prefect Deployment Guide for DLH-AQI Pipeline# Prefect Deployment Guide



Complete guide for deploying, scheduling, and running Prefect flows on YARN cluster.## 🚀 Cài đặt



## 📋 Table of Contents```bash

- [Quick Start](#quick-start)# Install Prefect và dependencies

- [Prerequisites](#prerequisites)pip install -r requirements.txt

- [YARN Configuration](#yarn-configuration)

- [Deployment](#deployment)# Hoặc cài riêng Prefect

- [Scheduling](#scheduling)pip install prefect>=3.0.0 python-dateutil>=2.8.2

- [Monitoring](#monitoring)```

- [Troubleshooting](#troubleshooting)

## 📋 Các Flows Có Sẵn

---

### 1. Silver Transformation Flow

## Quick Start**File:** `jobs/silver/prefect_silver_flow.py`



```bashChuyển đổi dữ liệu từ Bronze → Silver với:

# 1. Deploy flows with hourly schedule- Date key và time key enrichment

python Prefect/deploy.py --all- MERGE INTO operation (idempotent)

- Tự động retry khi lỗi

# 2. Start Prefect server (Terminal 1)- Validation step

prefect server start

# UI: http://localhost:4200**Sử dụng:**

```bash

# 3. Start agent (Terminal 2)# Chạy toàn bộ dữ liệu

prefect agent start -p default-agent-poolpython jobs/silver/prefect_silver_flow.py



# 4. Test manual run# Chạy với date range cụ thể

bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourlypython jobs/silver/prefect_silver_flow.py --date-range 2024-10-01 2024-10-31

```

# Backfill theo tháng (cho dataset lớn)

---python jobs/silver/prefect_silver_flow.py --backfill-monthly --date-range 2024-01-01 2024-12-31



## Prerequisites# Skip validation

python jobs/silver/prefect_silver_flow.py --skip-validation

### Software Requirements```



```bash### 2. Gold Dimension Flow

# Python 3.8+**File:** `jobs/gold/prefect_gold_flow.py`

python --version

Load các dimension tables với:

# Install dependencies- Parallel execution (location & pollutant cùng lúc)

pip install -r requirements.txt- Selective loading (--only, --skip)

- Aggregated metrics

# Or manually:

pip install prefect>=2.0.0 pyspark apache-iceberg-spark**Sử dụng:**

pip install openmeteo-requests pandas requests-cache retry-requests python-dotenv```bash

```# Load tất cả dimensions

python jobs/gold/prefect_gold_flow.py

### YARN Cluster Setup

# Chỉ load một số dimensions

✅ YARN ResourceManager accessible  python jobs/gold/prefect_gold_flow.py --only location pollutant

✅ HDFS access for warehouse (`/warehouse/iceberg`)  

✅ Spark libraries in HDFS (`/user/$USER/spark-libs.zip`)  # Skip một số dimensions

✅ Network access to Open-Meteo APIpython jobs/gold/prefect_gold_flow.py --skip time



### Environment Configuration (.env)# Chạy full pipeline (Bronze → Silver → Gold)

python jobs/gold/prefect_gold_flow.py --full-pipeline --date-range 2024-10-01 2024-10-31

```bash```

WAREHOUSE_URI=hdfs://khoa-master:9000/warehouse/iceberg

SPARK_MASTER=yarn## 🔧 Prefect Server Setup

ENABLE_YARN_DEFAULTS=true

SPARK_DYN_MIN=1### Option 1: Local Development (Recommended)

SPARK_DYN_MAX=50```bash

SPARK_AQE_ENABLED=true# Start Prefect server

```prefect server start



---# Trong terminal khác, config Prefect

prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

## YARN Configuration

# Xem UI tại: http://localhost:4200

### Spark Submit Wrapper```



The `scripts/spark_submit.sh` wrapper handles all YARN configuration automatically:### Option 2: Prefect Cloud

```bash

```bash# Login to Prefect Cloud

spark-submit \\prefect cloud login

  --master yarn \\

  --deploy-mode client \\# Hoặc set API key

  --conf spark.dynamicAllocation.enabled=true \\prefect config set PREFECT_API_KEY="your-api-key"

  --conf spark.sql.adaptive.enabled=true \\```

  --conf spark.dynamicAllocation.minExecutors=1 \\

  --conf spark.dynamicAllocation.maxExecutors=50 \\## 📦 Deployment

  --conf spark.yarn.archive=hdfs://khoa-master:9000/user/dlhnhom2/spark-libs.zip \\

  "$@"### 1. Deploy Silver Flow

``````bash

# Create deployment

### ⚠️ CRITICAL: Always Use spark_submit.shprefect deployment build jobs/silver/prefect_silver_flow.py:silver_transformation_flow \

    --name "silver-transformation" \

```bash    --tag "silver" \

# ❌ WRONG - Creates local SparkSession    --tag "production"

python Prefect/bronze_flow.py

# Apply deployment

# ✅ CORRECT - Submits to YARNprefect deployment apply silver_transformation_flow-deployment.yaml

bash scripts/spark_submit.sh Prefect/bronze_flow.py -- --mode upsert

```# Run deployment

prefect deployment run 'Silver Layer Transformation/silver-transformation'

### Verify YARN Setup```



```bash### 2. Deploy Gold Flow

# Test submission```bash

bash scripts/spark_submit.sh Prefect/bronze_flow.py -- --mode upsert --no-yarn-check# Create deployment

prefect deployment build jobs/gold/prefect_gold_flow.py:gold_dimension_flow \

# Check logs for: Master: yarn    --name "gold-dimensions" \

```    --tag "gold" \

    --tag "production"

---

# Apply deployment

## Deploymentprefect deployment apply gold_dimension_flow-deployment.yaml



### Step 1: Verify Flows# Run deployment

prefect deployment run 'Gold Dimension Loading/gold-dimensions'

Test each flow individually:```



```bash### 3. Deploy Full Pipeline

# Bronze```bash

bash scripts/spark_submit.sh Prefect/bronze_flow.py -- --mode upsert --no-yarn-check# Create deployment with schedule

prefect deployment build jobs/gold/prefect_gold_flow.py:full_pipeline_flow \

# Silver    --name "full-pipeline-daily" \

bash scripts/spark_submit.sh Prefect/silver_flow.py -- --mode incremental --no-yarn-check    --tag "pipeline" \

    --cron "0 2 * * *"  # Chạy lúc 2h sáng mỗi ngày

# Gold

bash scripts/spark_submit.sh Prefect/gold_flow.py -- --mode all --no-yarn-check# Apply deployment

prefect deployment apply full_pipeline_flow-deployment.yaml

# Full pipeline```

bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourly --no-yarn-check

```## 🔄 Scheduling



### Step 2: Create Deployments### Tạo schedule cho deployment

```python

```bashfrom prefect.deployments import Deployment

# Deploy all flowsfrom prefect.server.schemas.schedules import CronSchedule

python Prefect/deploy.py --all

# Schedule: Chạy mỗi ngày lúc 2h sáng

# Or individual flowsdeployment = Deployment(

python Prefect/deploy.py --flow hourly    name="full-pipeline-daily",

python Prefect/deploy.py --flow full    flow_name="Full Silver-Gold Pipeline",

python Prefect/deploy.py --flow backfill    schedule=CronSchedule(cron="0 2 * * *", timezone="Asia/Ho_Chi_Minh")

)

# Update existing deployment```

python Prefect/deploy.py --flow hourly --update

```### Hoặc dùng CLI:

```bash

### Step 3: Start Infrastructure# Thêm schedule cho deployment

prefect deployment set-schedule 'Full Silver-Gold Pipeline/full-pipeline-daily' \

#### Terminal 1: Prefect Server    --cron "0 2 * * *" \

    --timezone "Asia/Ho_Chi_Minh"

```bash

prefect server start# Pause/Resume deployment

# Access UI: http://localhost:4200prefect deployment pause 'Full Silver-Gold Pipeline/full-pipeline-daily'

```prefect deployment resume 'Full Silver-Gold Pipeline/full-pipeline-daily'

```

#### Terminal 2: Prefect Agent

## 📊 Monitoring

```bash

prefect agent start -p default-agent-pool### Xem flow runs

``````bash

# List all flow runs

The agent will:prefect flow-run ls

- Poll for scheduled/manual runs

- Execute flows via spark_submit.sh# Filter by flow name

- Report results back to serverprefect flow-run ls --flow-name "Silver Layer Transformation"



---# Filter by state

prefect flow-run ls --state COMPLETED

## Schedulingprefect flow-run ls --state FAILED

```

### Hourly Pipeline (Production)

### Logs

**Deployment:** `aqi-pipeline-hourly`  ```bash

**Schedule:** `0 * * * *` (every hour at minute 0)  # View logs for a flow run

**Timezone:** Asia/Ho_Chi_Minh (UTC+7)prefect flow-run logs <flow-run-id>



**What it runs:**# Stream logs in real-time

- Bronze: Upsert mode (update from latest)prefect flow-run logs <flow-run-id> --follow

- Silver: Incremental mode (merge)```

- Gold: Update all (dims + facts)

### UI Dashboard

**Configuration:**- Open browser: http://localhost:4200

- View:

```python  - Flow runs history

# Prefect/deploy.py  - Task execution times

schedule=CronSchedule(  - Success/failure rates

    cron="0 * * * *",  - Logs and artifacts

    timezone="Asia/Ho_Chi_Minh"

)## 🛠️ Advanced Usage

```

### 1. Custom retry logic

### Manual Runs```python

@task(

#### Via Prefect CLI    retries=3,

    retry_delay_seconds=[60, 300, 900],  # Exponential backoff

```bash    retry_condition_fn=lambda task, task_run, state: "timeout" in str(state)

# Run hourly pipeline)

prefect deployment run 'hourly-pipeline-flow/aqi-pipeline-hourly'def my_task():

    pass

# Run backfill```

prefect deployment run 'backfill-flow/aqi-pipeline-backfill' \\

  --param start_date=2024-01-01 \\### 2. Task caching

  --param end_date=2024-12-31 \\```python

  --param chunk_mode=monthlyfrom prefect.tasks import task_input_hash

from datetime import timedelta

# Run full pipeline with custom settings

prefect deployment run 'full-pipeline-flow/aqi-pipeline-full' \\@task(

  --param bronze_mode=upsert \\    cache_key_fn=task_input_hash,

  --param silver_mode=incremental \\    cache_expiration=timedelta(hours=1)

  --param gold_mode=all)

```def cached_task(param1, param2):

    # Task result sẽ được cache 1 giờ

#### Via Prefect UI    pass

```

1. Navigate to http://localhost:4200

2. Go to "Deployments"### 3. Notifications

3. Select deployment```bash

4. Click "Run" → "Quick run" or "Custom run"# Setup Slack notification

5. Set parameters if neededprefect block register -m prefect_slack



#### Via Direct YARN Submission# Hoặc email

prefect block register -m prefect_email

```bash```

# Bypass Prefect, run directly on YARN

bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourly### 4. Parameterized runs

bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\```bash

  --start-date 2024-01-01 \\# Run với parameters

  --end-date 2024-12-31 \\prefect deployment run 'Silver Layer Transformation/silver-transformation' \

  --chunk-mode monthly    --param start_date='2024-11-01' \

```    --param end_date='2024-11-30'

```

---

## 🐛 Troubleshooting

## Monitoring

### Issue: "No work pool found"

### 1. Prefect UI (http://localhost:4200)```bash

# Create default work pool

**Flow Runs:**prefect work-pool create default-agent-pool --type process

- Success/failure counts

- Duration per run# Start agent

- Parameters usedprefect agent start --pool default-agent-pool

```

**Tasks:**

- Individual task status### Issue: Tasks không chạy parallel

- Retry attempts- Check task runner: `ConcurrentTaskRunner()` được dùng?

- Error messages with stack traces- Check futures: Dùng `.submit()` cho parallel tasks



**Metrics:**### Issue: Import errors

- Records processed```bash

- SparkSession validation# Ensure PYTHONPATH includes src/

- Custom metrics from flowsexport PYTHONPATH="${PYTHONPATH}:${PWD}/src"



### 2. YARN ResourceManager (http://khoa-master:8088)# Hoặc add vào script:

import sys

- Application IDssys.path.insert(0, '/path/to/src')

- Resource allocation```

- Container logs

- Application progress## 📈 Performance Tuning



### 3. Spark History Server (http://khoa-master:18080)### 1. Increase parallel tasks

```python

- Stage timingsfrom prefect.task_runners import ConcurrentTaskRunner

- Task distribution

- Shuffle metrics@flow(task_runner=ConcurrentTaskRunner(max_workers=10))

- SQL query plansdef my_flow():

    pass

### 4. Custom Metrics Example```



Each flow returns structured metrics:### 2. Database optimization

```bash

```python# Config Prefect database

{prefect config set PREFECT_API_DATABASE_CONNECTION_URL="postgresql://user:pass@host/prefect"

    "success": True,```

    "mode": "monthly",

    "total_chunks": 12,### 3. Result persistence

    "successful_chunks": 12,```python

    "failed_chunks": [],from prefect.filesystems import LocalFileSystem

    "total_bronze_rows": 1234567,

    "total_silver_rows": 1234567,# Store results to disk

    "total_gold_records": 5432,@task(persist_result=True, result_storage=LocalFileSystem())

    "elapsed_seconds": 1234.5def my_task():

}    pass

``````



---## 🔐 Production Best Practices



## Troubleshooting1. **Environment variables**: Dùng `.env` file cho credentials

2. **Error handling**: Wrap critical operations trong try/except

### Issue: SparkSession not on YARN3. **Logging**: Dùng `print()` trong tasks → tự động vào Prefect logs

4. **Retry logic**: Set reasonable retries cho transient errors

**Symptom:**5. **Monitoring**: Setup alerts cho failed flows

```6. **Testing**: Test flows locally trước khi deploy

Master: local[*]7. **Documentation**: Document parameters và expected behavior

RuntimeError: Expected YARN master but got 'local[*]'

```## 📚 Resources



**Solution:**- [Prefect Docs](https://docs.prefect.io/)

- Always use `bash scripts/spark_submit.sh` wrapper- [Prefect Cloud](https://app.prefect.cloud/)

- Never run `python Prefect/...` directly- [Prefect Slack Community](https://prefect.io/slack)


### Issue: Multiple SparkSessions

**Symptom:**
```
Creating session: bronze_ingestion_flow
Creating session: silver_transformation_flow
```

**This is EXPECTED:**
- Each flow (bronze, silver, gold) has its own session
- Within a flow, session is reused across tasks
- Use `full_pipeline_flow.py` for single session across all stages

### Issue: HDFS Connection Timeout

**Solution:**
```bash
# Check HDFS
hdfs dfs -ls /user/$USER/data/

# Verify namenode
jps | grep NameNode
```

### Issue: Deployment Not Found

**Solution:**
```bash
# Re-deploy
python Prefect/deploy.py --flow hourly --update

# Verify
prefect deployment ls
```

### Issue: Agent Not Picking Up Runs

**Solution:**
```bash
# Check agent
prefect agent ls

# Restart
prefect agent start -p default-agent-pool
```

### Issue: Out of Memory on YARN

**Solution:**
Edit `scripts/spark_submit.sh`:

```bash
--conf spark.executor.memory=8g \\
--conf spark.driver.memory=4g \\
--conf spark.yarn.executor.memoryOverhead=2g
```

### Issue: Backfill Chunks Failing Randomly

**Solution:**
```bash
# Use smaller chunks
--chunk-mode weekly  # Instead of monthly

# Or add delay in backfill_flow.py
time.sleep(10)  # After each chunk
```

---

## Best Practices

### Development Workflow

```bash
# 1. Test locally
bash scripts/spark_submit.sh Prefect/bronze_flow.py -- \\
  --mode upsert --no-yarn-check

# 2. Test on YARN (small range)
bash scripts/spark_submit.sh Prefect/bronze_flow.py -- \\
  --mode backfill \\
  --start-date 2024-10-01 \\
  --end-date 2024-10-07

# 3. Deploy
python Prefect/deploy.py --flow bronze

# 4. Monitor in UI
# http://localhost:4200
```

### Production Deployment

✅ Always use `--update` to avoid duplicate deployments  
✅ Test with `--no-yarn-check` before full deployment  
✅ Start with small date ranges for backfills  
✅ Monitor first few scheduled runs  
✅ Set up alerting (Prefect Cloud optional)

### Maintenance

```bash
# Weekly: Check failed runs
prefect flow-run ls --state-type FAILED

# Monthly: Clean old data
prefect database clean --before-date 2024-01-01

# As needed: Update deployments
python Prefect/deploy.py --all --update
```

---

## Architecture Overview

```
Prefect Server (Orchestration)
     │
     ├─── Hourly Flow (Scheduled: 0 * * * *)
     ├─── Full Flow (Manual)
     └─── Backfill Flow (Manual)
          │
          ▼
     spark_submit.sh (YARN Wrapper)
          │
          ├─── Bronze Flow → Bronze Layer
          ├─── Silver Flow → Silver Layer
          └─── Gold Flow → Gold Layer
```

### Key Design Principles

1. **Single SparkSession Per Flow**
   - One session per flow execution
   - Reused across all tasks
   - Automatic cleanup

2. **YARN Validation**
   - Verifies `master == "yarn"`
   - Prevents local mode accidents
   - Optional skip for testing

3. **Idempotent Operations**
   - Bronze: Deduplication
   - Silver: MERGE INTO
   - Gold: Overwrite mode

---

## Additional Resources

- [Prefect Documentation](https://docs.prefect.io/)
- [PySpark on YARN](https://spark.apache.org/docs/latest/running-on-yarn.html)
- [Apache Iceberg](https://iceberg.apache.org/)
- [Project README](../README.md)
- [Flow Usage Guide](../PREFECT_FLOWS_README.md)
