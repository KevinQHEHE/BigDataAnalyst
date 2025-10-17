# Hướng dẫn Schedule Pipeline với Prefect + YARN

## 🎯 Tổng quan

Hệ thống này kết hợp **Prefect** (scheduling + monitoring) với **YARN** (execution) để có được ưu điểm của cả hai:

- ✅ **Prefect**: UI đẹp, scheduling linh hoạt, retry tự động, monitoring
- ✅ **YARN**: Distributed execution, resource management, cluster computing

## 🏗️ Kiến trúc

```
Prefect Schedule (mỗi giờ)
    ↓
Prefect Worker (process)
    ↓
yarn_wrapper_flow.py (subprocess)
    ↓
scripts/spark_submit.sh (spark-submit)
    ↓
YARN Cluster (distributed execution)
    ↓
Bronze → Silver → Gold pipeline
```

## 🚀 Quick Start

### 1. Deploy flow với schedule

```bash
# Deploy flow với schedule mỗi giờ
bash scripts/deploy_yarn_flow.sh
```

Kết quả:
```
✓ Deployment Complete!
Deployed flow:
  • hourly-yarn-pipeline → Runs every hour on YARN (cron: 0 * * * *)
```

### 2. Start Prefect worker (nếu chưa chạy)

```bash
# Start worker trong background
nohup prefect worker start --pool default > logs/prefect-worker.log 2>&1 &

# Kiểm tra worker đang chạy
ps aux | grep "prefect worker"
```

### 3. Test chạy ngay (không cần đợi)

```bash
# Trigger manual run
prefect deployment run "Hourly Pipeline on YARN/hourly-yarn-pipeline"

# Xem logs real-time
tail -f logs/prefect-worker.log
```

### 4. Monitor

```bash
# Xem flow runs
prefect flow-run ls --limit 10

# Xem chi tiết 1 run
prefect flow-run inspect <run-id>

# Hoặc mở UI
# http://localhost:4200
```

## 📋 Các lệnh quản lý

### Deployment Management

```bash
# Xem tất cả deployments
prefect deployment ls

# Xem chi tiết deployment
prefect deployment inspect "Hourly Pipeline on YARN/hourly-yarn-pipeline"

# Pause schedule (tạm dừng)
prefect deployment pause "Hourly Pipeline on YARN/hourly-yarn-pipeline"

# Resume schedule (tiếp tục)
prefect deployment resume "Hourly Pipeline on YARN/hourly-yarn-pipeline"

# Xóa deployment
prefect deployment delete "Hourly Pipeline on YARN/hourly-yarn-pipeline"
```

### Worker Management

```bash
# Start worker
nohup prefect worker start --pool default > logs/prefect-worker.log 2>&1 &

# Check worker status
ps aux | grep "prefect worker"

# Stop worker
pkill -f "prefect worker"

# View worker logs
tail -f logs/prefect-worker.log

# View recent errors in logs
tail -200 logs/prefect-worker.log | grep -i error
```

### Flow Run Management

```bash
# Trigger manual run
prefect deployment run "Hourly Pipeline on YARN/hourly-yarn-pipeline"

# List recent runs
prefect flow-run ls --limit 20

# Inspect specific run
prefect flow-run inspect <run-id>

# Watch run in real-time
prefect deployment run "Hourly Pipeline on YARN/hourly-yarn-pipeline" --watch

# Cancel running flow
prefect flow-run cancel <run-id>
```

## 🔧 Tùy chỉnh Schedule

### Thay đổi tần suất chạy

Sửa file `scripts/deploy_yarn_flow.sh`, thay `--cron "0 * * * *"`:

```bash
# Chạy 2 tiếng 1 lần
--cron "0 */2 * * *"

# Chạy 30 phút 1 lần  
--cron "*/30 * * * *"

# Chạy 4 giờ 1 lần
--cron "0 */4 * * *"

# Chạy mỗi ngày lúc 1:00 AM
--cron "0 1 * * *"

# Chỉ chạy thứ 2-6 (weekdays) lúc 8:00 AM
--cron "0 8 * * 1-5"
```

Sau đó deploy lại:
```bash
bash scripts/deploy_yarn_flow.sh
```

### Thêm parameters

Sửa file `Prefect/yarn_wrapper_flow.py` để nhận thêm parameters:

```python
@flow
def hourly_pipeline_yarn_flow(
    locations: str = "all",  # Thêm parameter
    debug: bool = False
) -> Dict:
    result = run_pipeline_on_yarn_task(
        flow_script="Prefect/full_pipeline_flow.py",
        mode="hourly",
        extra_args=f"--locations {locations}"  # Truyền vào command
    )
    return result
```

## 📊 Monitoring

### 1. Prefect UI

Mở trình duyệt: **http://localhost:4200**

Các tính năng:
- Dashboard với tổng quan flows
- Lịch sử runs (success/failure)
- Logs chi tiết từng run
- Gantt chart cho task execution
- Metrics & statistics

### 2. Command Line

```bash
# Xem flow runs gần đây
prefect flow-run ls --limit 10

# Xem chỉ failed runs
prefect flow-run ls --state FAILED

# Xem scheduled runs
prefect flow-run ls --state SCHEDULED

# Xem running runs
prefect flow-run ls --state RUNNING
```

### 3. Logs

```bash
# Worker logs (Prefect layer)
tail -f logs/prefect-worker.log

# Pipeline logs (application layer) - nếu có
ls -lht logs/hourly_pipeline_*.log | head -5
```

### 4. YARN UI

Mở trình duyệt: **http://khoa-master:8088/**

Xem:
- Running applications
- Resource usage (memory, CPU, containers)
- Application logs từ YARN
- Queue status

## 🔍 Troubleshooting

### Flow không chạy theo schedule?

```bash
# 1. Kiểm tra deployment có schedule không
prefect deployment inspect "Hourly Pipeline on YARN/hourly-yarn-pipeline" | grep -A5 schedule

# 2. Kiểm tra deployment có bị pause không
prefect deployment inspect "Hourly Pipeline on YARN/hourly-yarn-pipeline" | grep paused

# 3. Resume nếu bị pause
prefect deployment resume "Hourly Pipeline on YARN/hourly-yarn-pipeline"

# 4. Kiểm tra worker có chạy không
ps aux | grep "prefect worker"

# 5. Restart worker nếu cần
pkill -f "prefect worker"
nohup prefect worker start --pool default > logs/prefect-worker.log 2>&1 &
```

### Flow run failed?

```bash
# 1. Xem logs chi tiết
prefect flow-run inspect <run-id>

# 2. Xem logs trong worker
tail -200 logs/prefect-worker.log | grep -i error

# 3. Check YARN logs
# Mở YARN UI và tìm application tương ứng

# 4. Manual retry
prefect deployment run "Hourly Pipeline on YARN/hourly-yarn-pipeline"
```

### Worker crash?

```bash
# 1. Check worker process
ps aux | grep "prefect worker"

# 2. View worker logs
tail -100 logs/prefect-worker.log

# 3. Restart worker
nohup prefect worker start --pool default > logs/prefect-worker.log 2>&1 &
```

### Pipeline stuck?

```bash
# 1. Check YARN applications
yarn application -list

# 2. Kill specific application
yarn application -kill <application-id>

# 3. Cancel Prefect flow run
prefect flow-run cancel <run-id>
```

## 🎨 Advanced: Custom Flows

### Tạo flow mới cho backfill

```python
# Thêm vào Prefect/yarn_wrapper_flow.py

@flow(name="Backfill Pipeline on YARN")
def backfill_pipeline_yarn_flow(
    start_date: str = "2025-01-01",
    end_date: str = "2025-01-31"
) -> Dict:
    """Backfill pipeline for specific date range."""
    
    result = run_pipeline_on_yarn_task(
        flow_script="Prefect/backfill_flow.py",
        mode="custom"
    )
    return result
```

Deploy:
```bash
prefect deploy Prefect/yarn_wrapper_flow.py:backfill_pipeline_yarn_flow \
    --name "backfill-yarn" \
    --pool default
```

Run on-demand:
```bash
prefect deployment run "Backfill Pipeline on YARN/backfill-yarn" \
    --param start_date=2025-01-01 \
    --param end_date=2025-01-31
```

## 📝 Best Practices

### 1. Timeout & Retries

Flow đã có cấu hình:
- **Timeout**: 1 hour (3600s) per run
- **Retries**: 2 lần với delay 5 phút
- **Flow-level retry**: 1 lần với delay 10 phút

Để thay đổi, sửa `Prefect/yarn_wrapper_flow.py`:

```python
@task(retries=3, retry_delay_seconds=600, timeout_seconds=7200)
def run_pipeline_on_yarn_task(...):
    ...
```

### 2. Error Notifications

Thêm notifications khi flow failed:

```bash
# Tạo notification block (webhook, email, slack)
prefect block register -m prefect_slack  # Ví dụ với Slack

# Hoặc dùng webhooks trong flow
```

### 3. Monitoring Schedule

```bash
# Đặt cron job để check worker health
*/10 * * * * pgrep -f "prefect worker" || /path/to/restart_worker.sh
```

### 4. Log Rotation

```bash
# Trong crontab, rotate logs hàng ngày
0 0 * * * find /home/dlhnhom2/dlh-aqi/logs -name "*.log" -mtime +7 -delete
```

## 📚 So sánh với Cron

| Tiêu chí | Prefect + YARN | Cron + YARN |
|----------|----------------|-------------|
| Setup complexity | ⭐⭐⭐ Medium | ⭐ Easy |
| UI Monitoring | ✅ Excellent | ❌ No UI |
| Retry logic | ✅ Built-in | ❌ Manual |
| Workflow dependencies | ✅ Native | ⚠️ Shell scripts |
| YARN execution | ✅ Via wrapper | ✅ Direct |
| Resource overhead | ⭐⭐ Worker + Server | ⭐⭐⭐ Minimal |
| Debugging | ⭐⭐ Good (UI + logs) | ⭐⭐⭐ Simple logs |
| Production ready | ✅ Yes | ✅ Yes |

## 🎯 Kết luận

**Dùng Prefect + YARN khi:**
- ✅ Cần UI monitoring đẹp
- ✅ Cần retry logic phức tạp
- ✅ Có nhiều flows với dependencies
- ✅ Team muốn centralized orchestration
- ✅ Có resource chạy Prefect 24/7

**Hệ thống hiện tại:**
- ✅ Flow wrapper chạy `spark-submit` → YARN execution
- ✅ Prefect handle scheduling, retry, monitoring
- ✅ Best of both worlds!

## 📞 Support

Questions? Check:
- Prefect logs: `tail -f logs/prefect-worker.log`
- YARN UI: http://khoa-master:8088
- Prefect UI: http://localhost:4200
- Prefect docs: https://docs.prefect.io/
