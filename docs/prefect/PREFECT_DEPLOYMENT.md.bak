# Prefect Deployment Guide

## 🚀 Cài đặt

```bash
# Install Prefect và dependencies
pip install -r requirements.txt

# Hoặc cài riêng Prefect
pip install prefect>=3.0.0 python-dateutil>=2.8.2
```

## 📋 Các Flows Có Sẵn

### 1. Silver Transformation Flow
**File:** `jobs/silver/prefect_silver_flow.py`

Chuyển đổi dữ liệu từ Bronze → Silver với:
- Date key và time key enrichment
- MERGE INTO operation (idempotent)
- Tự động retry khi lỗi
- Validation step

**Sử dụng:**
```bash
# Chạy toàn bộ dữ liệu
python jobs/silver/prefect_silver_flow.py

# Chạy với date range cụ thể
python jobs/silver/prefect_silver_flow.py --date-range 2024-10-01 2024-10-31

# Backfill theo tháng (cho dataset lớn)
python jobs/silver/prefect_silver_flow.py --backfill-monthly --date-range 2024-01-01 2024-12-31

# Skip validation
python jobs/silver/prefect_silver_flow.py --skip-validation
```

### 2. Gold Dimension Flow
**File:** `jobs/gold/prefect_gold_flow.py`

Load các dimension tables với:
- Parallel execution (location & pollutant cùng lúc)
- Selective loading (--only, --skip)
- Aggregated metrics

**Sử dụng:**
```bash
# Load tất cả dimensions
python jobs/gold/prefect_gold_flow.py

# Chỉ load một số dimensions
python jobs/gold/prefect_gold_flow.py --only location pollutant

# Skip một số dimensions
python jobs/gold/prefect_gold_flow.py --skip time

# Chạy full pipeline (Bronze → Silver → Gold)
python jobs/gold/prefect_gold_flow.py --full-pipeline --date-range 2024-10-01 2024-10-31
```

## 🔧 Prefect Server Setup

### Option 1: Local Development (Recommended)
```bash
# Start Prefect server
prefect server start

# Trong terminal khác, config Prefect
prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"

# Xem UI tại: http://localhost:4200
```

### Option 2: Prefect Cloud
```bash
# Login to Prefect Cloud
prefect cloud login

# Hoặc set API key
prefect config set PREFECT_API_KEY="your-api-key"
```

## 📦 Deployment

### 1. Deploy Silver Flow
```bash
# Create deployment
prefect deployment build jobs/silver/prefect_silver_flow.py:silver_transformation_flow \
    --name "silver-transformation" \
    --tag "silver" \
    --tag "production"

# Apply deployment
prefect deployment apply silver_transformation_flow-deployment.yaml

# Run deployment
prefect deployment run 'Silver Layer Transformation/silver-transformation'
```

### 2. Deploy Gold Flow
```bash
# Create deployment
prefect deployment build jobs/gold/prefect_gold_flow.py:gold_dimension_flow \
    --name "gold-dimensions" \
    --tag "gold" \
    --tag "production"

# Apply deployment
prefect deployment apply gold_dimension_flow-deployment.yaml

# Run deployment
prefect deployment run 'Gold Dimension Loading/gold-dimensions'
```

### 3. Deploy Full Pipeline
```bash
# Create deployment with schedule
prefect deployment build jobs/gold/prefect_gold_flow.py:full_pipeline_flow \
    --name "full-pipeline-daily" \
    --tag "pipeline" \
    --cron "0 2 * * *"  # Chạy lúc 2h sáng mỗi ngày

# Apply deployment
prefect deployment apply full_pipeline_flow-deployment.yaml
```

## 🔄 Scheduling

### Tạo schedule cho deployment
```python
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule

# Schedule: Chạy mỗi ngày lúc 2h sáng
deployment = Deployment(
    name="full-pipeline-daily",
    flow_name="Full Silver-Gold Pipeline",
    schedule=CronSchedule(cron="0 2 * * *", timezone="Asia/Ho_Chi_Minh")
)
```

### Hoặc dùng CLI:
```bash
# Thêm schedule cho deployment
prefect deployment set-schedule 'Full Silver-Gold Pipeline/full-pipeline-daily' \
    --cron "0 2 * * *" \
    --timezone "Asia/Ho_Chi_Minh"

# Pause/Resume deployment
prefect deployment pause 'Full Silver-Gold Pipeline/full-pipeline-daily'
prefect deployment resume 'Full Silver-Gold Pipeline/full-pipeline-daily'
```

## 📊 Monitoring

### Xem flow runs
```bash
# List all flow runs
prefect flow-run ls

# Filter by flow name
prefect flow-run ls --flow-name "Silver Layer Transformation"

# Filter by state
prefect flow-run ls --state COMPLETED
prefect flow-run ls --state FAILED
```

### Logs
```bash
# View logs for a flow run
prefect flow-run logs <flow-run-id>

# Stream logs in real-time
prefect flow-run logs <flow-run-id> --follow
```

### UI Dashboard
- Open browser: http://localhost:4200
- View:
  - Flow runs history
  - Task execution times
  - Success/failure rates
  - Logs and artifacts

## 🛠️ Advanced Usage

### 1. Custom retry logic
```python
@task(
    retries=3,
    retry_delay_seconds=[60, 300, 900],  # Exponential backoff
    retry_condition_fn=lambda task, task_run, state: "timeout" in str(state)
)
def my_task():
    pass
```

### 2. Task caching
```python
from prefect.tasks import task_input_hash
from datetime import timedelta

@task(
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1)
)
def cached_task(param1, param2):
    # Task result sẽ được cache 1 giờ
    pass
```

### 3. Notifications
```bash
# Setup Slack notification
prefect block register -m prefect_slack

# Hoặc email
prefect block register -m prefect_email
```

### 4. Parameterized runs
```bash
# Run với parameters
prefect deployment run 'Silver Layer Transformation/silver-transformation' \
    --param start_date='2024-11-01' \
    --param end_date='2024-11-30'
```

## 🐛 Troubleshooting

### Issue: "No work pool found"
```bash
# Create default work pool
prefect work-pool create default-agent-pool --type process

# Start agent
prefect agent start --pool default-agent-pool
```

### Issue: Tasks không chạy parallel
- Check task runner: `ConcurrentTaskRunner()` được dùng?
- Check futures: Dùng `.submit()` cho parallel tasks

### Issue: Import errors
```bash
# Ensure PYTHONPATH includes src/
export PYTHONPATH="${PYTHONPATH}:${PWD}/src"

# Hoặc add vào script:
import sys
sys.path.insert(0, '/path/to/src')
```

## 📈 Performance Tuning

### 1. Increase parallel tasks
```python
from prefect.task_runners import ConcurrentTaskRunner

@flow(task_runner=ConcurrentTaskRunner(max_workers=10))
def my_flow():
    pass
```

### 2. Database optimization
```bash
# Config Prefect database
prefect config set PREFECT_API_DATABASE_CONNECTION_URL="postgresql://user:pass@host/prefect"
```

### 3. Result persistence
```python
from prefect.filesystems import LocalFileSystem

# Store results to disk
@task(persist_result=True, result_storage=LocalFileSystem())
def my_task():
    pass
```

## 🔐 Production Best Practices

1. **Environment variables**: Dùng `.env` file cho credentials
2. **Error handling**: Wrap critical operations trong try/except
3. **Logging**: Dùng `print()` trong tasks → tự động vào Prefect logs
4. **Retry logic**: Set reasonable retries cho transient errors
5. **Monitoring**: Setup alerts cho failed flows
6. **Testing**: Test flows locally trước khi deploy
7. **Documentation**: Document parameters và expected behavior

## 📚 Resources

- [Prefect Docs](https://docs.prefect.io/)
- [Prefect Cloud](https://app.prefect.cloud/)
- [Prefect Slack Community](https://prefect.io/slack)
