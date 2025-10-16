# Prefect Flows - Hệ Thống Orchestration cho DLH-AQI Pipeline

> **Workflow orchestration cho pipeline xử lý dữ liệu chất lượng không khí trên YARN cluster**

Thư mục này chứa các Prefect flows để điều phối pipeline Bronze → Silver → Gold với:
- ✅ **Single SparkSession** - Một session duy nhất cho mỗi flow
- ✅ **YARN Integration** - Chạy trên Hadoop cluster qua subprocess wrapper
- ✅ **Auto-detect Optimization** - Chỉ xử lý dữ liệu mới
- ✅ **Streaming Output** - Giảm memory usage khi chạy jobs lớn
- ✅ **Automatic Retry** - Tự động retry khi lỗi
- ✅ **Hourly Scheduling** - Tự động chạy mỗi giờ

---

## 📋 Mục Lục

- [Kiến Trúc Hệ Thống](#-kiến-trúc-hệ-thống)
- [Chi Tiết Các Files](#-chi-tiết-các-files)
- [Cấu Hình và Tham Số](#-cấu-hình-và-tham-số)
- [Hướng Dẫn Sử Dụng](#-hướng-dẫn-sử-dụng)
- [Deployment](#-deployment)
- [Monitoring](#-monitoring)
- [Troubleshooting](#-troubleshooting)

---

## 🏗️ Kiến Trúc Hệ Thống

### Flow Dependency Graph

```
┌────────────────────────────────────────────────────────────┐
│                 yarn_wrapper_flow.py                       │
│         (Prefect Scheduler → YARN Executor)                │
│                                                             │
│  ┌──────────────────────────────────────────────────────┐ │
│  │   Subprocess: bash scripts/spark_submit.sh           │ │
│  │                                                       │ │
│  │   ┌────────────────────────────────────────────┐    │ │
│  │   │      full_pipeline_flow.py                 │    │ │
│  │   │  (Single SparkSession trên YARN)           │    │ │
│  │   │                                            │    │ │
│  │   │  ┌──────────┐  ┌──────────┐  ┌──────────┐│    │ │
│  │   │  │ Bronze   │─▶│ Silver   │─▶│  Gold    ││    │ │
│  │   │  └──────────┘  └──────────┘  └──────────┘│    │ │
│  │   └────────────────────────────────────────────┘    │ │
│  └──────────────────────────────────────────────────────┘ │
└────────────────────────────────────────────────────────────┘

Alternative: backfill_flow.py (cho historical data)
```

### Execution Flow

```
User/Cron
   │
   ▼
Prefect Worker (Local Python)
   │
   │ Calls
   ▼
yarn_wrapper_flow.py (@flow)
   │
   │ Subprocess
   ▼
bash scripts/spark_submit.sh
   │
   │ Submit to YARN
   ▼
YARN Cluster (Spark Application)
   │
   ▼
full_pipeline_flow.py
   ├─▶ bronze_flow.py
   ├─▶ silver_flow.py
   └─▶ gold_flow.py
```

---

## 📁 Chi Tiết Các Files

### 1. `spark_context.py` - SparkSession Context Manager

**Mục đích**: Quản lý SparkSession với context manager pattern

**Class chính**: `SparkSessionContext`

```python
# Usage
with get_spark_session(app_name="my_flow", require_yarn=True) as spark:
    df = spark.sql("SELECT * FROM table")
    # Session tự động stop khi exit context
```

**Chức năng**:
- ✅ Tạo single SparkSession cho mỗi flow
- ✅ Validate YARN mode (`master == "yarn"`)
- ✅ Tự động stop session khi hoàn thành
- ✅ Log thông tin Spark (App ID, Master, Warehouse)

**Parameters**:
- `app_name` (str): Tên application
- `require_yarn` (bool): Bắt buộc phải chạy trên YARN, raise error nếu không
- `mode` (Optional[str]): Override mode ("local" cho testing, None cho production)

**Validation**:
```python
def validate_yarn_mode(spark: SparkSession):
    """Kiểm tra xem Spark có đang chạy trên YARN không"""
    master = spark.sparkContext.master
    if not master.startswith("yarn"):
        raise RuntimeError(f"Expected YARN but got '{master}'")
```

**Best Practice**:
- Luôn dùng context manager (`with` statement)
- Set `require_yarn=True` cho production flows
- Set `require_yarn=False` cho sub-flows (đã validated ở parent)

---

### 2. `yarn_wrapper_flow.py` - Prefect ↔ YARN Bridge

**Mục đích**: Wrapper để Prefect schedule và monitor Spark jobs trên YARN

**Lý do cần wrapper**:
- Prefect worker chạy **local Python** (không phải trên YARN)
- Spark jobs cần chạy trên **YARN cluster**
- Wrapper dùng **subprocess** để gọi `spark-submit`

**Architecture**:
```
Prefect Worker (Local)
    │
    │ @task
    ▼
run_pipeline_on_yarn_task()
    │
    │ subprocess.Popen (streaming output)
    ▼
bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py
    │
    │ spark-submit --master yarn
    ▼
YARN Cluster (Distributed)
```

**Key Features**:

1. **Streaming Output** (Optimized):
```python
process = subprocess.Popen(
    cmd,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,  # Merge streams
    text=True,
    bufsize=1  # Line buffered
)

# Stream line by line (không buffer toàn bộ vào memory)
output_lines = []
max_lines_to_keep = 100  # Chỉ giữ 100 dòng cuối

for line in process.stdout:
    print(line, end='')  # Print ngay lập tức
    output_lines.append(line)
    if len(output_lines) > max_lines_to_keep:
        output_lines.pop(0)  # Xóa dòng cũ
```

**Lợi ích**:
- ✅ Giảm memory usage (không lưu toàn bộ output)
- ✅ Real-time output (thấy progress ngay)
- ✅ Giữ 100 dòng cuối để debug nếu lỗi

2. **Timeout và Cleanup**:
```python
return_code = process.wait(timeout=3600)  # 1 hour timeout

except subprocess.TimeoutExpired:
    if process.poll() is None:
        process.kill()  # Kill process nếu timeout
        process.wait()
```

3. **Retry on Failure**:
```python
@task(name="run_pipeline_on_yarn", retries=2, retry_delay_seconds=300)
```

**Flows**:
- `hourly_pipeline_yarn_flow()`: Chạy hourly pipeline (mode="hourly")
- `full_pipeline_yarn_flow()`: Chạy full pipeline (mode="full")

**Command được execute**:
```bash
bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourly
```

---

### 3. `bronze_flow.py` - Bronze Layer Ingestion

**Mục đích**: Ingest dữ liệu từ Open-Meteo API → Bronze layer (Iceberg)

**Architecture**:
```python
@flow("Bronze Ingestion Flow")
def bronze_ingestion_flow():
    with get_spark_session("bronze_flow", require_yarn=True) as spark:
        # 1. Load locations
        locations = load_locations_task()
        
        # 2. Ingest each location
        for location in locations:
            ingest_location_chunk_task(location, ...)
        
        # 3. Return metrics
        return {total_rows, elapsed_seconds, ...}
```

**Modes**:

1. **Upsert Mode** (Default):
```bash
bash scripts/spark_submit.sh Prefect/bronze_flow.py -- --mode upsert
```
- Tìm timestamp mới nhất trong Bronze
- Chỉ ingest từ `latest_timestamp + 1 day` đến hôm nay
- Dùng cho **hourly pipeline**

2. **Backfill Mode**:
```bash
bash scripts/spark_submit.sh Prefect/bronze_flow.py -- \\
  --mode backfill \\
  --start-date 2024-01-01 \\
  --end-date 2024-12-31
```
- Ingest toàn bộ date range
- Chia thành chunks (default 90 ngày/chunk)
- Dùng cho **historical data**

**Tasks**:
- `load_locations_task`: Load danh sách locations từ HDFS
- `get_latest_timestamp_task`: Lấy timestamp mới nhất cho location
- `ingest_location_chunk_task`: Gọi API và write vào Iceberg

**Parameters**:
```python
--mode: "upsert" hoặc "backfill"
--locations: Path to locations.jsonl (default HDFS)
--start-date: Start date cho backfill (YYYY-MM-DD)
--end-date: End date cho backfill (YYYY-MM-DD)
--chunk-days: Số ngày mỗi API request (default 90)
--override: Ghi đè data cũ (default False)
--table: Target Iceberg table
--warehouse: Iceberg warehouse URI
--no-yarn-check: Skip YARN validation (testing only)
```

**Output Example**:
```
BRONZE FLOW COMPLETE
================================================================================
Total rows ingested: 2,160
Locations processed: 3
Elapsed time: 45.2s
Total rows in table: 47,160
```

---

### 4. `silver_flow.py` - Silver Layer Transformation

**Mục đích**: Transform Bronze → Silver với data enrichment và quality checks

**Architecture**:
```python
@flow("Silver Transformation Flow")
def silver_transformation_flow():
    with get_spark_session("silver_flow", require_yarn=True) as spark:
        # 1. Transform Bronze → Silver
        transform_result = transform_bronze_to_silver_task(
            mode="merge",  # Upsert với merge
            auto_detect=True  # Chỉ xử lý data mới
        )
        
        # 2. Validate data quality (optional)
        if not skip_validation:
            validation_result = validate_silver_task()
        
        return {transformation, validation, elapsed}
```

**Key Features**:

1. **Auto-detect Optimization**:
```python
transform_bronze_to_silver(
    spark=spark,
    mode="merge",
    auto_detect=True  # Tự động detect MAX(ts_utc)
)
```
- Compare `MAX(ts_utc)` giữa Bronze vs Silver
- Skip nếu không có data mới
- Chỉ process rows mới nếu có

2. **Data Enrichment**:
- Add `date_key` (YYYYMMDD format)
- Add `time_key` (0-23 hours)
- Parse pollutant metrics (PM2.5, PM10, O3, NO2, SO2, CO, UV)
- Calculate AQI from PM2.5

3. **Write Modes**:
- `overwrite`: Xóa và ghi lại toàn bộ
- `merge`: Upsert (update existing, insert new)
- `append`: Chỉ insert (không update)

**Tasks**:
- `transform_bronze_to_silver_task`: Main transformation logic
- `validate_silver_task`: Data quality checks

**Validation Checks**:
```python
- Total records count
- Duplicates trên (location_key, ts_utc)
- NULL values trong date_key, time_key, location_key
- Output: validation_passed = True/False
```

**Parameters**:
```python
--mode: "full" (overwrite) hoặc "incremental" (merge)
--start-date: Filter start date (optional)
--end-date: Filter end date (optional)
--skip-validation: Bỏ qua validation step
--bronze-table: Source table name
--silver-table: Target table name
--warehouse: Iceberg warehouse URI
--no-yarn-check: Skip YARN validation
```

**Output Example**:
```
SILVER FLOW COMPLETE
================================================================================
Records processed: 1,080
Elapsed time: 23.5s
Validation: PASSED (0 duplicates, 0 nulls)
```

---

### 5. `gold_flow.py` - Gold Layer Pipeline

**Mục đích**: Load dimensions + Transform facts cho Gold layer (analytical data)

**Architecture**:
```python
@flow("Gold Pipeline Flow")
def gold_pipeline_flow(mode="all"):
    with get_spark_session("gold_flow", require_yarn=True) as spark:
        # 1. Load Dimensions (parallel)
        if "dims" in mode:
            load_dim_location_task()
            load_dim_pollutant_task()
            generate_dim_time_task()
            generate_dim_date_task()
        
        # 2. Transform Facts (sequential, có dependency)
        if "facts" in mode:
            transform_fact_hourly_task(auto_detect=True)
            transform_fact_daily_task(auto_detect=True)
            detect_episodes_task()
        
        return {results, total_records, elapsed}
```

**Modes**:

| Mode | Dimensions | Facts | Use Case |
|------|------------|-------|----------|
| `all` | ✅ All 4 | ✅ All 3 | Full refresh |
| `dims` | ✅ All 4 | ❌ None | Only dims |
| `facts` | ❌ None | ✅ All 3 | Only facts (recommended for backfill) |
| `custom` | ✅ Selected | ✅ Selected | Flexible selection |

**Dimension Tables** (Reference Data):

1. **dim_location** (3 rows):
```sql
location_key | location_name | latitude | longitude
-------------|---------------|----------|----------
danang       | Da Nang       | 16.0544  | 108.2022
hanoi        | Ha Noi        | 21.0285  | 105.8542
hcmc         | Ho Chi Minh   | 10.8231  | 106.6297
```

2. **dim_pollutant** (10 rows):
```sql
pollutant_key | pollutant_name | unit  | description
--------------|----------------|-------|-------------
pm25          | PM2.5          | µg/m³ | Fine particles
pm10          | PM10           | µg/m³ | Coarse particles
...
```

3. **dim_time** (24 rows):
```sql
time_key | hour | period    | is_peak
---------|------|-----------|--------
0        | 0    | Night     | False
...
17       | 17   | Evening   | True  (5-7pm)
```

4. **dim_date** (655 rows):
```sql
date_key | date_utc   | year | month | day | day_of_week
---------|------------|------|-------|-----|------------
20231001 | 2023-10-01 | 2023 | 10    | 1   | Sunday
...
```

**Fact Tables** (Analytical Data):

1. **fact_air_quality_hourly** (47,160 rows):
```sql
SELECT 
    location_key,
    date_key,
    time_key,
    ts_utc,
    pm25, pm10, o3, no2, so2, co,
    aqi, aod, dust, uv_index
FROM hadoop_catalog.lh.gold.fact_air_quality_hourly
```

2. **fact_city_daily** (1,965 rows):
```sql
-- Daily aggregations by city
SELECT
    location_key,
    date_key,
    avg_pm25, max_pm25, min_pm25,
    avg_aqi, max_aqi,
    total_hours, hours_good, hours_moderate, ...
FROM hadoop_catalog.lh.gold.fact_city_daily
```

3. **fact_episode** (396 rows):
```sql
-- Pollution episodes (AQI > threshold for min_hours)
SELECT
    episode_id,
    location_key,
    start_ts, end_ts,
    duration_hours,
    avg_aqi, max_aqi,
    severity_level
FROM hadoop_catalog.lh.gold.fact_episode
WHERE aqi_threshold = 151  -- Unhealthy threshold
  AND duration_hours >= 4
```

**Auto-detect trong Facts**:
```python
# fact_hourly
transform_fact_hourly(spark, auto_detect=True)
# → Compare MAX(ts_utc): Silver vs Gold
# → Skip nếu không có data mới

# fact_daily
transform_fact_daily(spark, auto_detect=True)
# → Compare MAX(date_key): fact_hourly vs fact_daily
# → Chỉ aggregate dates mới
```

**Parameters**:
```python
--mode: "all", "dims", "facts", hoặc "custom"
--dims: Comma-separated dims cho custom (location,pollutant,time,date)
--facts: Comma-separated facts cho custom (hourly,daily,episode)
--locations: Path to locations.jsonl
--pollutants: Path to dim_pollutant.jsonl
--aqi-threshold: Threshold cho episode detection (default 151)
--min-hours: Minimum giờ liên tục cho episode (default 4)
--warehouse: Iceberg warehouse URI
--no-yarn-check: Skip YARN validation
```

**Output Example**:
```
GOLD FLOW COMPLETE
================================================================================
  dim_location: 3 records
  dim_pollutant: 10 records
  dim_time: 24 records
  dim_date: 655 records
  fact_hourly: 47,160 records
  fact_daily: 1,965 records
  fact_episode: 396 records

Total records: 50,213
Elapsed time: 67.8s
```

---

### 6. `full_pipeline_flow.py` - Complete Pipeline

**Mục đích**: Orchestrate Bronze → Silver → Gold trong **single SparkSession**

**Architecture**:
```python
@flow("Full Pipeline Flow")
def full_pipeline_flow():
    # Create ONE SparkSession for entire pipeline
    with get_spark_session("full_pipeline", require_yarn=True) as spark:
        
        # Stage 1: Bronze (nếu không skip)
        if not skip_bronze:
            bronze_result = bronze_ingestion_flow(
                mode=bronze_mode,
                require_yarn=False  # Already validated
            )
        
        # Stage 2: Silver (nếu không skip)
        if not skip_silver:
            silver_result = silver_transformation_flow(
                mode=silver_mode,
                require_yarn=False  # Already validated
            )
        
        # Stage 3: Gold (nếu không skip)
        if not skip_gold:
            gold_result = gold_pipeline_flow(
                mode=gold_mode,
                require_yarn=False  # Already validated
            )
        
        return {success, results, elapsed}
```

**Key Benefits**:
- ✅ **Single SparkSession** - Không overhead tạo nhiều sessions
- ✅ **Shared context** - Catalog, configs được reuse
- ✅ **Efficient** - Giảm cluster resource churn
- ✅ **Transactional** - Tất cả stages trong 1 Spark application

**Error Handling**:
```python
try:
    bronze_result = bronze_ingestion_flow(...)
    results["bronze"] = bronze_result
except Exception as e:
    results["bronze"] = {"success": False, "error": str(e)}
    # Continue to next stage (không stop)
```

**Use Cases**:

1. **Hourly Scheduled Run**:
```bash
bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourly
# → Bronze: upsert
# → Silver: incremental
# → Gold: facts only
```

2. **Manual Full Refresh**:
```bash
bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- \\
  --bronze-mode backfill \\
  --bronze-start-date 2024-10-01 \\
  --bronze-end-date 2024-10-31 \\
  --silver-mode incremental \\
  --gold-mode all
```

3. **Skip Stages**:
```bash
# Chỉ chạy Silver + Gold
bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- \\
  --skip-bronze \\
  --silver-mode full \\
  --gold-mode all
```

**Parameters**:
```python
# Bronze
--bronze-mode: "upsert" hoặc "backfill"
--bronze-start-date: Start date cho backfill
--bronze-end-date: End date cho backfill
--skip-bronze: Bỏ qua Bronze stage

# Silver
--silver-mode: "full" hoặc "incremental"
--silver-start-date: Filter start date
--silver-end-date: Filter end date
--skip-validation: Bỏ qua validation
--skip-silver: Bỏ qua Silver stage

# Gold
--gold-mode: "all", "dims", "facts", "custom"
--skip-gold: Bỏ qua Gold stage
--aqi-threshold: Episode threshold
--min-hours: Episode minimum hours

# Common
--locations: Path to locations.jsonl
--pollutants: Path to dim_pollutant.jsonl
--warehouse: Iceberg warehouse URI
--no-yarn-check: Skip YARN validation
```

**Output Example**:
```
FULL PIPELINE FLOW: BRONZE → SILVER → GOLD
================================================================================
Pipeline stages:
  Bronze: UPSERT
  Silver: INCREMENTAL
  Gold: FACTS
================================================================================

STAGE 1/3: BRONZE INGESTION
✓ Bronze stage complete: 1,080 rows

STAGE 2/3: SILVER TRANSFORMATION
✓ Silver stage complete: 1,080 rows

STAGE 3/3: GOLD PIPELINE
✓ Gold stage complete: 49,521 records

FULL PIPELINE COMPLETE
================================================================================
✓ BRONZE: {'success': True, 'total_rows': 1080}
✓ SILVER: {'success': True, 'records_processed': 1080}
✓ GOLD: {'success': True, 'total_records': 49521}

Total pipeline time: 92.8s
Overall status: SUCCESS
```

---

### 7. `backfill_flow.py` - Historical Data Backfill

**Mục đích**: Xử lý large date ranges với chunking strategy

**Problem**: Backfill 1 năm data (365 ngày) cùng lúc:
- ❌ API rate limiting
- ❌ Memory issues
- ❌ Timeout risks
- ❌ Hard to track progress

**Solution**: Chia thành chunks nhỏ:
```
2024-01-01 to 2024-12-31 (1 năm)
  ↓ monthly chunking
  ├─ 2024-01-01 to 2024-01-31 (chunk 1)
  ├─ 2024-02-01 to 2024-02-29 (chunk 2)
  ├─ ...
  └─ 2024-12-01 to 2024-12-31 (chunk 12)
```

**Architecture**:
```python
@flow("Backfill Flow")
def backfill_flow(start_date, end_date, chunk_mode):
    # Generate chunks
    chunks = generate_date_chunks(start_date, end_date, chunk_mode)
    # → [(2024-01-01, 2024-01-31), (2024-02-01, 2024-02-29), ...]
    
    with get_spark_session("backfill_flow", require_yarn=True) as spark:
        # Process each chunk
        for i, (chunk_start, chunk_end) in enumerate(chunks):
            # Bronze
            bronze_result = bronze_ingestion_flow(
                mode="backfill",
                start_date=chunk_start,
                end_date=chunk_end,
                override=True  # Ghi đè data cũ
            )
            
            # Silver
            silver_result = silver_transformation_flow(
                mode="incremental",  # Merge mode
                start_date=chunk_start,
                end_date=chunk_end
            )
        
        # Gold (chạy 1 lần sau khi tất cả chunks xong)
        if successful_chunks > 0:
            gold_result = gold_pipeline_flow(
                mode="facts"  # Chỉ facts, skip dimensions
            )
        
        return {chunks, successful, failed, elapsed}
```

**Chunking Modes**:

1. **Monthly** (Default):
```bash
--chunk-mode monthly
# 2024-01-01 to 2024-12-31 → 12 chunks
```

2. **Weekly**:
```bash
--chunk-mode weekly
# 2024-01-01 to 2024-12-31 → 53 chunks (7 days each)
```

3. **Daily**:
```bash
--chunk-mode daily
# 2024-10-01 to 2024-10-15 → 15 chunks (1 day each)
```

**Gold Mode Optimization**:
```python
# Before: mode="all"
# → Reload dimensions (692 rows) + facts (47,160 rows) = 50,213 total
# → 9.3 minutes

# After: mode="facts"
# → Skip dimensions, only process facts with auto_detect
# → 1.5 minutes (84% faster!)
```

**Parameters**:
```python
--start-date: Start date (YYYY-MM-DD) - REQUIRED
--end-date: End date (YYYY-MM-DD) - REQUIRED
--chunk-mode: "monthly", "weekly", hoặc "daily"
--skip-bronze: Bỏ qua Bronze stage
--skip-silver: Bỏ qua Silver stage
--skip-gold: Bỏ qua Gold stage
--locations: Path to locations.jsonl
--pollutants: Path to dim_pollutant.jsonl
--warehouse: Iceberg warehouse URI
--no-yarn-check: Skip YARN validation
```

**Usage Examples**:

```bash
# Backfill 1 năm by month
bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
  --start-date 2024-01-01 \\
  --end-date 2024-12-31 \\
  --chunk-mode monthly

# Backfill 1 tháng by week
bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
  --start-date 2024-10-01 \\
  --end-date 2024-10-31 \\
  --chunk-mode weekly

# Backfill 15 ngày by day
bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
  --start-date 2025-10-01 \\
  --end-date 2025-10-15 \\
  --chunk-mode daily
```

**Output Example**:
```
BACKFILL FLOW: HISTORICAL DATA PROCESSING
================================================================================
Date range: 2025-10-01 to 2025-10-15
Chunk mode: daily
Stages: Bronze → Silver → Gold
================================================================================

Generated 15 chunks:
  Chunk   1: 2025-10-01 to 2025-10-01
  Chunk   2: 2025-10-02 to 2025-10-02
  ...
  Chunk  15: 2025-10-15 to 2025-10-15

PROCESSING CHUNK 1/15: 2025-10-01 to 2025-10-01
[Chunk 1] Bronze ingestion...
[Chunk 1] ✓ Bronze: 72 rows
[Chunk 1] Silver transformation...
[Chunk 1] ✓ Silver: 72 rows
[Chunk 1] ✓ Complete in 6.2s

...

GOLD PIPELINE (after all chunks)
✓ Gold complete: 49,521 records

BACKFILL COMPLETE
================================================================================
Date range: 2025-10-01 to 2025-10-15
Chunk mode: daily
Total chunks: 15
Successful: 15
Failed: 0

Data processed:
  Bronze rows: 1,080
  Silver rows: 1,080
  Gold records: 49,521

Total time: 92.8s (1.5 minutes)
Average per chunk: 6.2s
================================================================================
```

---

## ⚙️ Cấu Hình và Tham Số

### Environment Variables (.env)

```bash
# Iceberg warehouse
WAREHOUSE_URI=hdfs://khoa-master:9000/warehouse/iceberg

# Spark settings
SPARK_MASTER=yarn
ENABLE_YARN_DEFAULTS=true
SPARK_DYN_MIN=1
SPARK_DYN_MAX=50

# Data paths
LOCATIONS_PATH=hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl
POLLUTANTS_PATH=hdfs://khoa-master:9000/user/dlhnhom2/data/dim_pollutant.jsonl
```

### Spark Configuration (scripts/spark_submit.sh)

```bash
spark-submit \\
  --master yarn \\
  --deploy-mode client \\
  --conf spark.dynamicAllocation.enabled=true \\
  --conf spark.dynamicAllocation.minExecutors=1 \\
  --conf spark.dynamicAllocation.maxExecutors=50 \\
  --conf spark.sql.adaptive.enabled=true \\
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \\
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog \\
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop \\
  --conf spark.sql.catalog.hadoop_catalog.warehouse=$WAREHOUSE_URI \\
  "$@"
```

### Prefect Configuration

**Work Pool**: `default` (process type)
```bash
prefect work-pool create default --type process
```

**Schedule**: Cron every hour
```bash
--cron "0 * * * *"  # Minute 0 của mỗi giờ
```

**Tags**: Để filter và organize
```bash
--tag aqi --tag hourly --tag yarn --tag production
```

---

## 🚀 Hướng Dẫn Sử Dụng

### Setup Ban Đầu

**1. Install Dependencies**:
```bash
pip install prefect==3.4.22
pip install pyspark
pip install python-dotenv
```

**2. Tạo Work Pool**:
```bash
prefect work-pool create default --type process
```

**3. Deploy Flow**:
```bash
bash scripts/deploy_yarn_flow.sh
```

**4. Start Worker**:
```bash
# Chạy trong background
nohup prefect worker start --pool default > logs/prefect-worker.log 2>&1 &

# Lưu PID
echo $! > logs/prefect-worker.pid
```

### Pattern 1: Scheduled Hourly Runs (Production)

**Automatic**: Worker tự động chạy theo schedule

```
17:00 → Prefect worker trigger → yarn_wrapper_flow
         ↓
     subprocess: bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourly
         ↓
     YARN: Bronze (upsert) → Silver (incremental) → Gold (facts)
```

**Monitor**:
```bash
# Xem scheduled runs
prefect flow-run ls --limit 10

# Xem worker logs
tail -f logs/prefect-worker.log

# Check worker status
ps aux | grep "prefect worker"
```

### Pattern 2: Manual Trigger

**Test run**:
```bash
prefect deployment run 'Hourly Pipeline on YARN/hourly-yarn-pipeline'
```

**Custom parameters** (via direct spark-submit):
```bash
bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- \\
  --bronze-mode upsert \\
  --silver-mode incremental \\
  --gold-mode facts \\
  --skip-validation
```

### Pattern 3: Historical Backfill

**Backfill 1 năm**:
```bash
bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
  --start-date 2024-01-01 \\
  --end-date 2024-12-31 \\
  --chunk-mode monthly
```

**Backfill 1 tháng**:
```bash
bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
  --start-date 2024-10-01 \\
  --end-date 2024-10-31 \\
  --chunk-mode weekly
```

### Pattern 4: Individual Layers

**Chỉ Bronze**:
```bash
bash scripts/spark_submit.sh Prefect/bronze_flow.py -- \\
  --mode backfill \\
  --start-date 2024-10-01 \\
  --end-date 2024-10-31
```

**Chỉ Silver**:
```bash
bash scripts/spark_submit.sh Prefect/silver_flow.py -- \\
  --mode incremental \\
  --start-date 2024-10-01 \\
  --end-date 2024-10-31
```

**Chỉ Gold - Facts only**:
```bash
bash scripts/spark_submit.sh Prefect/gold_flow.py -- \\
  --mode facts
```

**Chỉ Gold - Dimensions only**:
```bash
bash scripts/spark_submit.sh Prefect/gold_flow.py -- \\
  --mode dims
```

---

## 📦 Deployment

### Deploy Script: `scripts/deploy_yarn_flow.sh`

**Chức năng**:
1. Create work pool nếu chưa có
2. Delete old deployment (tránh duplicate)
3. Deploy với schedule + tags
4. Show next steps

**Usage**:
```bash
bash scripts/deploy_yarn_flow.sh
```

**Output**:
```
Deployed flow:
  • hourly-yarn-pipeline → Runs every hour on YARN (cron: 0 * * * *)

Deployment ID: 31505e99-d4de-4e41-9259-84401f3e5e8e
```

### Deployment Lifecycle

**1. Code Changes**:
- ✅ Sửa code trong `*.py` files → **KHÔNG CẦN** deploy lại
- ❌ Đổi schedule/tags/name → **CẦN** deploy lại

**2. Update Deployment**:
```bash
# Delete old
prefect deployment delete "Hourly Pipeline on YARN/hourly-yarn-pipeline"

# Redeploy
bash scripts/deploy_yarn_flow.sh
```

**3. Worker Restart**:
```bash
# Kill old worker
kill $(cat logs/prefect-worker.pid)

# Start new worker
nohup prefect worker start --pool default > logs/prefect-worker.log 2>&1 &
echo $! > logs/prefect-worker.pid
```

---

## 📊 Monitoring

### Prefect UI

**Access**: http://localhost:4200

**Features**:
- Flow runs history
- Task execution status
- Logs và errors
- Metrics và charts

**Useful Views**:
```bash
# Dashboard → Flow Runs (xem tất cả runs)
# Deployments → hourly-yarn-pipeline (xem schedule)
# Flow Runs → Click vào run → Task Runs (xem từng task)
```

### YARN ResourceManager

**Access**: http://khoa-master:8088

**Views**:
- Applications (Spark jobs đang chạy)
- Cluster metrics (CPU, Memory)
- Node list

### Spark History Server

**Access**: http://khoa-master:18080

**Views**:
- Completed applications
- Job timeline
- Stage và task details
- SQL queries

### Command Line Monitoring

**Flow runs**:
```bash
# List recent runs
prefect flow-run ls --limit 10

# Watch specific run
prefect flow-run logs <run-id> --follow
```

**Worker status**:
```bash
# Check if running
ps aux | grep "prefect worker"

# View logs
tail -f logs/prefect-worker.log

# Last 100 lines with timestamp
tail -n 100 logs/prefect-worker.log | grep -E "^\d{2}:\d{2}:\d{2}"
```

**YARN applications**:
```bash
# List running apps
yarn application -list -appStates RUNNING

# App status
yarn application -status <app-id>

# Kill app
yarn application -kill <app-id>
```

### Metrics to Track

| Metric | Source | Threshold |
|--------|--------|-----------|
| Flow duration | Prefect UI | < 2 minutes (hourly) |
| Success rate | Prefect UI | > 95% |
| Data freshness | Bronze table | < 2 hours lag |
| Worker uptime | `ps aux` | Continuous |
| YARN queue usage | YARN UI | < 80% |

---

## 🐛 Troubleshooting

### Common Issues

**1. Worker not picking up runs**

**Symptoms**:
```bash
prefect flow-run ls
# Shows: Scheduled → Pending (stuck)
```

**Solutions**:
```bash
# Check worker
ps aux | grep "prefect worker"

# Restart worker
kill $(cat logs/prefect-worker.pid)
nohup prefect worker start --pool default > logs/prefect-worker.log 2>&1 &
echo $! > logs/prefect-worker.pid

# Check logs
tail -f logs/prefect-worker.log
```

---

**2. SparkSession not on YARN**

**Error**:
```
RuntimeError: Expected YARN master but got 'local[*]'
```

**Cause**: Chạy trực tiếp với Python thay vì spark-submit

**Solution**:
```bash
# ❌ WRONG
python Prefect/bronze_flow.py

# ✅ CORRECT
bash scripts/spark_submit.sh Prefect/bronze_flow.py -- --mode upsert
```

---

**3. Subprocess timeout**

**Error**:
```
RuntimeError: Pipeline timed out after 3600.0s
```

**Cause**: Job chạy quá 1 giờ

**Solutions**:
```python
# Option 1: Tăng timeout trong yarn_wrapper_flow.py
return_code = process.wait(timeout=7200)  # 2 hours

# Option 2: Optimize query (enable auto_detect)
# Option 3: Reduce chunk size in backfill
```

---

**4. Memory issues in subprocess**

**Symptoms**: Process killed, OOM errors

**Cause**: Trước đây dùng `capture_output=True` buffer toàn bộ output

**Solution**: Đã fix bằng streaming output
```python
# ✅ FIXED: Stream line by line
for line in process.stdout:
    print(line, end='')
    # Chỉ giữ 100 dòng cuối
```

---

**5. HDFS connection timeout**

**Error**:
```
java.net.SocketTimeoutException: 60000 millis timeout
```

**Solutions**:
```bash
# Check HDFS
hdfs dfs -ls /user/dlhnhom2/

# Check NameNode
curl http://khoa-master:9870/

# Restart HDFS (if needed)
# sudo systemctl restart hadoop-hdfs-namenode
```

---

**6. Iceberg table locked**

**Error**:
```
org.apache.iceberg.exceptions.CommitFailedException: ...
```

**Cause**: Concurrent writes

**Solutions**:
```bash
# Check running Spark apps
yarn application -list -appStates RUNNING

# Kill stuck apps
yarn application -kill <app-id>
```

---

**7. Deployment prompt issues**

**Error**:
```
EOFError: EOF when reading a line
```

**Cause**: Script chạy non-interactive nhưng CLI cần input

**Solution**: Đã fix bằng `printf "n\nn\n"`
```bash
printf "n\nn\n" | prefect deploy ...
```

---

### Debug Workflow

**Step 1**: Check worker logs
```bash
tail -f logs/prefect-worker.log
```

**Step 2**: Check Prefect UI
```
http://localhost:4200 → Flow Runs → Click run → View logs
```

**Step 3**: Check YARN logs
```bash
yarn logs -applicationId <app-id> | less
```

**Step 4**: Check Spark History
```
http://khoa-master:18080 → Find application → Stages → Failed tasks
```

---

## 📚 Tài Liệu Tham Khảo

### Internal Docs
- **[PREFECT_YARN_GUIDE.md](../docs/PREFECT_YARN_GUIDE.md)** - Hướng dẫn tích hợp Prefect + YARN
- **[PREFECT_DEPLOYMENT.md](../docs/PREFECT_DEPLOYMENT.md)** - Chi tiết deployment
- **[INCREMENTAL_IMPLEMENTATION.md](../docs/INCREMENTAL_IMPLEMENTATION.md)** - Auto-detect optimization

### External Resources
- [Prefect Documentation](https://docs.prefect.io/)
- [Apache Iceberg](https://iceberg.apache.org/)
- [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)

---

## ✅ Checklist

### Pre-production
- [ ] Test full pipeline với small date range
- [ ] Verify YARN execution (`master == "yarn"`)
- [ ] Check auto-detect working (skips when no new data)
- [ ] Validate data quality (no duplicates, no nulls)
- [ ] Monitor memory usage (streaming output)

### Production
- [ ] Deploy với hourly schedule
- [ ] Start worker trong background
- [ ] Verify scheduled runs executing
- [ ] Set up monitoring alerts
- [ ] Document runbooks

### Maintenance
- [ ] Weekly: Check worker logs for errors
- [ ] Weekly: Verify data freshness
- [ ] Monthly: Review performance metrics
- [ ] Monthly: Clean up old Spark artifacts
- [ ] Quarterly: Update dependencies

---

## 🎓 Best Practices

### 1. Always use spark_submit.sh wrapper
```bash
# ✅ CORRECT
bash scripts/spark_submit.sh Prefect/bronze_flow.py -- --mode upsert

# ❌ WRONG
python Prefect/bronze_flow.py --mode upsert
```

### 2. Enable auto_detect for incremental loads
```python
transform_bronze_to_silver(spark, mode="merge", auto_detect=True)
transform_fact_hourly(spark, mode="overwrite", auto_detect=True)
```

### 3. Use mode="facts" for backfill Gold
```python
# Skip dimensions (reference data), only process facts
gold_pipeline_flow(mode="facts")
```

### 4. Chunk large date ranges
```bash
# ✅ Monthly chunks for 1 year
--chunk-mode monthly  # 12 chunks

# ❌ Process entire year at once (risky)
```

### 5. Monitor worker continuously
```bash
# Run in background with nohup
nohup prefect worker start --pool default > logs/prefect-worker.log 2>&1 &

# Save PID for easy management
echo $! > logs/prefect-worker.pid
```

### 6. Set require_yarn=False for sub-flows
```python
# Parent flow
with get_spark_session("parent", require_yarn=True) as spark:
    # Sub-flows reuse session
    bronze_flow(..., require_yarn=False)
    silver_flow(..., require_yarn=False)
```

---

## 📝 Notes

- **Code changes**: Không cần deploy lại, worker tự động dùng code mới
- **Metadata changes**: Cần deploy lại (schedule, tags, name)
- **Worker restart**: Chỉ cần khi có lỗi hoặc update Prefect version
- **YARN validation**: Set `require_yarn=True` cho parent flow, `False` cho sub-flows

---

**Last Updated**: October 16, 2025
**Author**: DLH-AQI Team
**Version**: 3.0 (Prefect 3.x + YARN Integration + Streaming Optimization)
