# 📚 Ingest Pipeline — Tài liệu Hoàn chỉnh

Hướng dẫn toàn bộ quy trình ingestion từ Bronze → Silver → Gold cho AQI Lakehouse pipeline.

---

## 📋 Tổng quan

Pipeline AQI sử dụng **Medallion Architecture** - 3 layers xử lý dữ liệu:

```
Open-Meteo API
     ↓ (fetch hourly)
Bronze Layer (raw data)
     ↓ (clean + add keys)
Silver Layer (data warehouse)
     ↓ (aggregate + enrich)
Gold Layer (analytics-ready)
```

| Layer | Thành phần | Input | Output | Mục đích |
|-------|-----------|-------|--------|---------|
| **Bronze** | `run_bronze_pipeline.py` | Open-Meteo API | `bronze.open_meteo_hourly` | Raw hourly data |
| **Silver** | `run_silver_pipeline.py` | Bronze | `silver.air_quality_hourly_clean` | Cleaned + keyed |
| **Gold** | `load_dim_*.py` + `transform_fact_*.py` | Silver | `gold.dim_*` + `gold.fact_*` | Star schema |

---

## 🚀 Quick Start

### ⚡ One-time Setup (Lần đầu)

```bash
# 1️⃣ Backfill Bronze với 1 năm dữ liệu lịch sử
bash scripts/spark_submit.sh jobs/bronze/run_bronze_pipeline.py -- \
  --mode backfill \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --chunk-days 90

# 2️⃣ Transform Bronze → Silver (full)
bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- --mode full

# 3️⃣ Load Gold Dimensions
bash scripts/spark_submit.sh jobs/gold/load_dim_date.py -- --mode full
bash scripts/spark_submit.sh jobs/gold/load_dim_time.py
bash scripts/spark_submit.sh jobs/gold/load_dim_location.py
bash scripts/spark_submit.sh jobs/gold/load_dim_pollutant.py

# 4️⃣ Transform Gold Facts
bash scripts/spark_submit.sh jobs/gold/transform_fact_hourly.py -- --mode full
bash scripts/spark_submit.sh jobs/gold/transform_fact_daily.py -- --mode full
bash scripts/spark_submit.sh jobs/gold/detect_episodes.py -- --mode full
```

### 🔄 Daily (Hàng ngày)

```bash
# 1️⃣ Bronze: Upsert new data from API
bash scripts/spark_submit.sh jobs/bronze/run_bronze_pipeline.py -- --mode upsert

# 2️⃣ Silver: Transform incremental
bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- --mode incremental

# 3️⃣ Gold: Refresh facts
bash scripts/spark_submit.sh jobs/gold/transform_fact_hourly.py -- --mode incremental
bash scripts/spark_submit.sh jobs/gold/transform_fact_daily.py -- --mode incremental
bash scripts/spark_submit.sh jobs/gold/detect_episodes.py -- --mode incremental
```

---

## 🏗️ Bronze Layer — API Data Ingestion

### 📌 Mục đích

**Lấy dữ liệu thô từ Open-Meteo Air Quality API** và lưu vào Iceberg table.

**Đặc điểm**:
- Hourly measurements từ 50+ locations worldwide
- ~47K+ records (~720 records/location/year)
- Raw data gần như API response (minimal transformation)
- Partition theo `date_utc` → efficient time-series queries

### 📁 File

- **Script**: `jobs/bronze/run_bronze_pipeline.py` (403 lines)
- **Input**: `data/locations.jsonl` (location metadata)
- **Output**: `hadoop_catalog.lh.bronze.open_meteo_hourly` (Iceberg table)
- **External**: Open-Meteo API (free, no key required)

### 🔧 Mode: Backfill

Nạp dữ liệu lịch sử. Dùng lần đầu setup hoặc reprocess data cũ.

```bash
# Local Spark
python3 jobs/bronze/run_bronze_pipeline.py \
  --mode backfill \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --chunk-days 90

# YARN Cluster (recommended for production)
bash scripts/spark_submit.sh jobs/bronze/run_bronze_pipeline.py -- \
  --mode backfill \
  --start-date 2024-01-01 \
  --end-date 2024-12-31 \
  --chunk-days 30 \
  --override
```

**Parameters**:
- `--start-date`, `--end-date`: Date range (YYYY-MM-DD format)
- `--chunk-days`: Split date range thành chunks (default 90)
  - Nhỏ hơn = safer, nhưng chậm hơn
  - Lớn hơn = nhanh hơn, nhưng risk memory/timeout
- `--override`: Ghi đè data cũ (optional, default False)
  - False = skip dates already exist
  - True = reprocess all dates
- `--locations`: Path tới locations file (default HDFS)
- `--table`: Target Iceberg table (default: `hadoop_catalog.lh.bronze.open_meteo_hourly`)

**Logic**:
1. Load danh sách locations từ JSONL (CSV-like format, line-by-line JSON)
2. Tách date range thành chunks nhỏ (ví dụ 90 ngày chunks tránh API timeout)
3. Với mỗi (location, chunk):
   - Check xem data đã tồn tại không (trừ khi `--override`)
   - Gọi Open-Meteo API fetch hourly data cho khoảng thời gian
   - Chuẩn hóa: NaN → NULL, cast sang type đúng (AQI → Int, PM → Double)
   - Deduplicate: `drop_duplicates(['location_key', 'ts_utc'])`
   - Append vào Iceberg table (không overwrite)
4. Sleep 1s giữa API calls (tránh rate limit)

**Ví dụ**:
```bash
# Backfill Q1 2024 (3 months)
bash scripts/spark_submit.sh jobs/bronze/run_bronze_pipeline.py -- \
  --mode backfill \
  --start-date 2024-01-01 \
  --end-date 2024-03-31 \
  --chunk-days 30

# Reprocess January (override old data)
bash scripts/spark_submit.sh jobs/bronze/run_bronze_pipeline.py -- \
  --mode backfill \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --override
```

### 🔧 Mode: Upsert (Daily Update)

Cập nhật data mới hàng ngày. Dùng sau backfill để maintain fresh data.

```bash
# Local Spark
python3 jobs/bronze/run_bronze_pipeline.py --mode upsert

# YARN Cluster
bash scripts/spark_submit.sh jobs/bronze/run_bronze_pipeline.py -- --mode upsert
```

**Logic**:
1. Load danh sách locations
2. Với mỗi location:
   - Tìm timestamp mới nhất (MAX(ts_utc)) trong bronze table
   - **Nếu không có data**: Skip location (cần backfill trước)
   - **Nếu có data**: Tính range từ (latest_ts + 1 day) đến hôm nay
   - Gọi API fetch data cho khoảng mới này
   - Append data mới (deduplicated)
3. Print summary: locations processed, total records

**Key behavior**:
- ✅ **Safe**: Chỉ append, không ghi đè data cũ
- ✅ **Idempotent**: Chạy lại cùng ngày không duplicate (dedupe ở pandas)
- ❌ **Requires backfill first**: Locations chưa có initial data sẽ bị skip
- ⏱️ **Daily schedule**: Dùng cho hàng ngày updates

**Cron setup** (chạy lúc 02:00 AM mỗi ngày):
```bash
0 2 * * * cd /home/dlhnhom2/dlh-aqi && \
  bash scripts/spark_submit.sh jobs/bronze/run_bronze_pipeline.py -- --mode upsert >> logs/bronze.log 2>&1
```

### 📊 Bronze Schema (47 columns)

```python
# Coordinates & Location (5 cols)
location_key (STRING)        # e.g., "hanoi"
ts_utc (TIMESTAMP)           # Hourly timestamp UTC (partition)
date_utc (DATE)              # Partition column for efficiency
latitude (DOUBLE)
longitude (DOUBLE)

# AQI Indices per US EPA (7 cols)
aqi (INT)                    # Overall AQI 0-500 range
aqi_pm25, aqi_pm10, aqi_no2, aqi_o3, aqi_so2, aqi_co (INT)

# Pollutant Concentrations in μg/m³ (6 cols)
pm25, pm10, o3, no2, so2, co (DOUBLE)

# Additional Environmental Parameters (4 cols)
aod (DOUBLE)                 # Aerosol Optical Depth
dust (DOUBLE)
uv_index (DOUBLE)
co2 (DOUBLE)

# Metadata (3 cols)
model_domain (STRING)
request_timezone (STRING)
_ingested_at (TIMESTAMP)     # Ingestion timestamp for audit
```

### 📈 Performance Expectations

| Operation | Duration | Notes |
|-----------|----------|-------|
| Backfill 1 location, 1 month | ~30s | 720 hourly records, 1 API call |
| Backfill 1 location, 1 year (90-day chunks) | ~5 min | 4 API calls (with 1s delays) |
| Backfill 50 locations, 1 year (90-day chunks) | ~4-5h | ~200 API calls total (1s delay) |
| Upsert 50 locations, 1 day | ~2-3 min | Incremental only new data |
| Write 100K records to Iceberg | ~10-15s | Optimized with coalesce |

---

## 🏗️ Silver Layer — Data Cleaning & Enrichment

### 📌 Mục đích

**Làm sạch & chuẩn hóa** dữ liệu từ Bronze:
- Thêm dimensional keys (`date_key`, `time_key`)
- Loại bỏ duplicates
- MERGE INTO cho idempotent upsert

**Kết quả**: Bảng `hadoop_catalog.lh.silver.air_quality_hourly_clean` - Analytics-ready.

### 📁 File

- **Script**: `jobs/silver/run_silver_pipeline.py` (313 lines)
- **Input**: `hadoop_catalog.lh.bronze.open_meteo_hourly`
- **Output**: `hadoop_catalog.lh.silver.air_quality_hourly_clean`

### 🔧 Mode: Full

Rebuild toàn bộ Silver table từ Bronze (first-time setup).

```bash
# Local Spark
python3 jobs/silver/run_silver_pipeline.py --mode full

# YARN Cluster
bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- --mode full
```

**Logic**:
1. Read **tất cả** dữ liệu từ Bronze table
2. Thêm dimensional keys:
   - `date_key`: Extract từ date_utc, format YYYYMMDD (e.g., 20240115)
   - `time_key`: Extract hour từ ts_utc, format HHMM (e.g., 1400 for 14:00)
3. Drop duplicates theo (location_key, ts_utc) — keep first/last?
4. **Overwrite** Silver table (replace all)

**Timing**: ~45-60 giây cho 47K records

### 🔧 Mode: Incremental

Cập nhật chỉ dữ liệu mới (sau Bronze upsert).

```bash
# Auto-detect new data in Bronze
python3 jobs/silver/run_silver_pipeline.py --mode incremental

# Explicit date range
bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- \
  --mode incremental \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

**Logic (auto-detect)**:
1. Find MAX(ts_utc) trong Silver table
2. Read Bronze WHERE ts_utc > max_ts
3. Transform (add keys) + Merge vào Silver (upsert mode)

**Logic (explicit range)**:
1. Filter Bronze WHERE date BETWEEN start_date AND end_date
2. Transform + Merge

**Key behavior**:
- ✅ **Idempotent**: MERGE vào, chạy lại cùng range không duplicate
- ✅ **Auto-detect**: Tự tìm data mới nếu không specify date
- ⏱️ **Daily schedule**: Chạy sau Bronze upsert

**Cron setup** (chạy 1h sau Bronze, lúc 03:00 AM):
```bash
0 3 * * * cd /home/dlhnhom2/dlh-aqi && \
  bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- --mode incremental >> logs/silver.log 2>&1
```

### 📊 Silver Schema

**Thêm 2 cột vào Bronze**:
- `date_key`: INT (YYYYMMDD format)
- `time_key`: INT (HHMM format, e.g., 1400)

**Preserve**: Tất cả 47 cột từ Bronze + 2 new = 49 cols total

**Deduplication key**: (location_key, ts_utc)

### 📈 Performance

| Operation | Duration | Records |
|-----------|----------|---------|
| Full load from Bronze | ~45-60s | 47K |
| Incremental (1 day) | ~10-15s | ~2K |
| Incremental (1 month) | ~30s | ~60K |

---

## 🏗️ Gold Layer — Star Schema

### 📌 Mục điff

**Tạo star schema** cho analytics: 4 dimension tables + 3 fact tables.

### 📁 Files

```
jobs/gold/
├── load_dim_date.py                    # Date dimension (365+ days)
├── load_dim_time.py                    # Time dimension (24 hours, static)
├── load_dim_location.py                # Location metadata
├── load_dim_pollutant.py               # Pollutant definitions
├── transform_fact_hourly.py            # Hourly air quality facts
├── transform_fact_daily.py             # Daily aggregates
└── detect_episodes.py                  # Episode detection algorithm
```

### 🔷 Dimensions (4 tables)

#### 1. dim_date (365+ records)

Date dimension với calendar attributes.

```bash
# Full load (rebuild from Silver unique dates)
bash scripts/spark_submit.sh jobs/gold/load_dim_date.py -- --mode full

# Incremental (add only new dates)
bash scripts/spark_submit.sh jobs/gold/load_dim_date.py -- --mode incremental
```

**Columns**:
- `date_key` (INT): YYYYMMDD format
- `date_value` (DATE): Actual date
- `day_of_month` (INT): 1-31
- `day_of_week` (INT): 1=Monday, 7=Sunday
- `week_of_year` (INT): 1-53
- `month` (INT): 1-12
- `month_name` (STRING): "January", "February", ...
- `quarter` (INT): 1-4
- `year` (INT): YYYY
- `is_weekend` (BOOLEAN): Saturday/Sunday = TRUE

#### 2. dim_time (24 records, static)

Mỗi giờ trong ngày.

```bash
bash scripts/spark_submit.sh jobs/gold/load_dim_time.py
```

**Columns**:
- `time_key` (INT): 0, 100, 200, ..., 2300
- `time_value` (STRING): "00:00", "01:00", ..., "23:00"
- `hour` (INT): 0-23
- `work_shift` (STRING): "night" | "morning" | "afternoon" | "evening"

**Shift definitions**:
- night: 00:00-05:59 (hours 0-5)
- morning: 06:00-11:59 (hours 6-11)
- afternoon: 12:00-17:59 (hours 12-17)
- evening: 18:00-23:59 (hours 18-23)

#### 3. dim_location (N records)

Location metadata.

```bash
bash scripts/spark_submit.sh jobs/gold/load_dim_location.py -- \
  --locations hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl
```

**Columns**:
- `location_key` (STRING)
- `location_name` (STRING)
- `latitude` (DOUBLE)
- `longitude` (DOUBLE)
- `timezone` (STRING): e.g., "Asia/Ho_Chi_Minh"

**Input**: `data/locations.jsonl` (JSONL format, one JSON per line)

#### 4. dim_pollutant (10 records)

Pollutant definitions.

```bash
bash scripts/spark_submit.sh jobs/gold/load_dim_pollutant.py -- \
  --pollutants hdfs://khoa-master:9000/user/dlhnhom2/data/dim_pollutant.jsonl
```

**Columns**:
- `pollutant_code` (STRING): "pm25", "pm10", "o3", etc.
- `display_name` (STRING): "Fine Particulate Matter", etc.
- `unit_default` (STRING): "μg/m³", etc.
- `aqi_timespan` (STRING, nullable): Averaging period for AQI

**Input**: `data/dim_pollutant.jsonl` (JSONL format)

### 🔶 Facts (3 tables)

#### 1. fact_air_quality_hourly (47K+ records)

Hourly measurements with enrichments.

```bash
# Full load
bash scripts/spark_submit.sh jobs/gold/transform_fact_hourly.py -- --mode full

# Incremental (auto-detect new Silver data)
bash scripts/spark_submit.sh jobs/gold/transform_fact_hourly.py -- --mode incremental

# Explicit date range
bash scripts/spark_submit.sh jobs/gold/transform_fact_hourly.py -- \
  --mode incremental \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

**Enrichments** (added from Silver):
- `dominant_pollutant`: argmax(aqi_pm25, aqi_pm10, aqi_o3, aqi_no2, aqi_so2, aqi_co)
  - Which pollutant drives overall AQI?
- `data_completeness`: (non-null pollutant columns / 10) * 100%
  - How much data we have for this hour?
- `record_id`: UUID for audit trail

**Schema**: Silver columns + enrichments (49 + 3 = 52 cols)

**Size**: ~47K records (all hours × all locations)

#### 2. fact_city_daily (1.9K+ records)

Daily aggregates: max AQI, hour counts by category.

```bash
bash scripts/spark_submit.sh jobs/gold/transform_fact_daily.py -- \
  --mode [full|incremental] \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

**Key Columns**:
- `location_key`, `date_utc`, `date_key`
- `aqi_daily_max`: MAX(aqi) per (location, date)
- `dominant_pollutant_daily`: Pollutant with highest AQI
- `hours_in_cat_*`: Hour counts by AQI category:
  - `hours_in_cat_good`: AQI 0-50 (how many hours)
  - `hours_in_cat_moderate`: AQI 51-100
  - `hours_in_cat_usg`: AQI 101-150 (Unhealthy for Sensitive Groups)
  - `hours_in_cat_unhealthy`: AQI 151-200
  - `hours_in_cat_very_unhealthy`: AQI 201-300
  - `hours_in_cat_hazardous`: AQI 301+
- `hours_measured`: Count of non-null AQI hours
- `data_completeness`: (hours_measured / 24) * 100%

**Size**: ~1.9K records (1 per location per date)

#### 3. fact_episode (396 episodes)

High AQI episodes: sustained periods ≥151 AQI for ≥4h.

```bash
# Default: AQI >= 151, duration >= 4 hours
bash scripts/spark_submit.sh jobs/gold/detect_episodes.py -- --mode full

# Custom thresholds
bash scripts/spark_submit.sh jobs/gold/detect_episodes.py -- \
  --mode full \
  --aqi-threshold 200 \
  --min-hours 6

# Incremental detection
bash scripts/spark_submit.sh jobs/gold/detect_episodes.py -- --mode incremental
```

**Key Columns**:
- `episode_id` (UUID): Unique identifier
- `location_key`, `start_ts_utc`, `end_ts_utc`
- `duration_hours`: Inclusive duration (end - start + 1)
- `peak_aqi`: MAX(aqi) during episode
- `hours_flagged`: Count hours in episode
- `dominant_pollutant`: Pollutant with highest AQI
- `rule_code`: e.g., "AQI>=151_4h"

**Algorithm**:
1. Flag hours where AQI ≥ threshold
2. Identify runs of consecutive flagged hours
3. Filter runs ≥ min_hours duration
4. Aggregate each run with metrics

**Size**: ~396 episodes (year data)

---

## ⚙️ Configuration

### Environment Variables (`.env`)

```bash
# Iceberg Warehouse
WAREHOUSE_URI=hdfs://khoa-master:9000/warehouse/iceberg

# Spark Master (leave empty for local Spark)
SPARK_MASTER=yarn

# Data Input Files
LOCATIONS_FILE=hdfs://khoa-master:9000/user/dlhnhom2/data/locations.jsonl
POLLUTANTS_FILE=hdfs://khoa-master:9000/user/dlhnhom2/data/dim_pollutant.jsonl
```

### Locations File Format

`data/locations.jsonl` — JSONL format (one JSON per line):

```jsonl
{"location_key":"hanoi","location_name":"Hà Nội","latitude":21.0278,"longitude":105.8342,"timezone":"Asia/Ho_Chi_Minh"}
{"location_key":"hcm","location_name":"TP. Hồ Chí Minh","latitude":10.7769,"longitude":106.7009,"timezone":"Asia/Ho_Chi_Minh"}
{"location_key":"danang","location_name":"Đà Nẵng","latitude":16.0544,"longitude":108.2022,"timezone":"Asia/Ho_Chi_Minh"}
```

**Required fields**: location_key, latitude, longitude
**Optional fields**: location_name, timezone

---

## 📈 Performance & Monitoring

### Expected Timing

| Operation | Duration | Notes |
|-----------|----------|-------|
| Bronze backfill 1 year (50 loc) | 4-5h | ~200 API calls with 1s delay |
| Silver full load | 45-60s | 47K records transform + dedupe |
| Silver incremental (1 day) | 10-15s | ~2K records |
| Gold dimensions load | ~30s | One-time only |
| Gold fact_hourly | ~1 min | Transform + UUID generation |
| Gold fact_daily aggregation | ~30s | GroupBy + aggregate |
| Gold episode detection | ~20s | Window function + flagging |
| **Total daily pipeline** | **~15 min** | All 3 layers incremental |

### Monitoring Queries

```bash
# Bronze: Row count by date
spark-sql -e "
  SELECT date_utc, COUNT(*) as count 
  FROM hadoop_catalog.lh.bronze.open_meteo_hourly 
  GROUP BY date_utc ORDER BY date_utc DESC LIMIT 10
"

# Silver: Deduplication check
spark-sql -e "
  SELECT 
    location_key,
    COUNT(*) as silver_records,
    COUNT(DISTINCT ts_utc) as unique_ts
  FROM hadoop_catalog.lh.silver.air_quality_hourly_clean
  GROUP BY location_key LIMIT 5
"

# Gold: Fact record counts
spark-sql -e "
  SELECT 
    'hourly' as fact_table,
    COUNT(*) as record_count
  FROM hadoop_catalog.lh.gold.fact_air_quality_hourly
  UNION ALL
  SELECT 'daily', COUNT(*) FROM hadoop_catalog.lh.gold.fact_city_daily
  UNION ALL
  SELECT 'episodes', COUNT(*) FROM hadoop_catalog.lh.gold.fact_episode
"

# Data quality: AQI distribution
spark-sql -e "
  SELECT 
    ROUND(aqi/50)*50 as aqi_bucket, 
    COUNT(*) as count
  FROM hadoop_catalog.lh.gold.fact_air_quality_hourly
  GROUP BY ROUND(aqi/50)*50
  ORDER BY aqi_bucket DESC
"
```

### Logs

```bash
# Real-time logs
tail -f logs/bronze.log logs/silver.log logs/gold_*.log

# Spark cluster status
yarn application -list
yarn application -status <app_id>

# HDFS usage
hdfs dfs -du -sh /warehouse/iceberg/hadoop_catalog/lh/
```

---

## 🐛 Troubleshooting

| Issue | Cause | Solution |
|-------|-------|----------|
| `Connection timeout` | API rate limit or network | Increase `--chunk-days`, reduce parallel locations |
| `MERGE fails: table not found` | Table not initialized | Run schema creation script first |
| `No new data detected` | Silver already up-to-date | Check Bronze upsert completed before Silver |
| `Memory error: Java heap` | Large date range processing | Reduce `--end-date` or increase executor memory |
| `HDFS safe mode` | Cluster maintenance | `hdfs dfsadmin -safemode leave` |
| `Duplicate records in output` | Dedup failed or skipped | Already handled by pandas `drop_duplicates()` |
| `NaN values in output` | API returned missing values | Expected, preserved as NULL in Iceberg |
| `Slow Spark performance` | Inefficient partitioning | Check Iceberg partition scheme, increase parallelism |

---

## 🔄 Recommended Workflow

### 🔧 Setup (One-time, ~6-8 hours)

```bash
# Phase 1: Backfill Bronze (4-5 hours)
for quarter in Q1 Q2 Q3 Q4; do
  bash scripts/spark_submit.sh jobs/bronze/run_bronze_pipeline.py -- \
    --mode backfill \
    --start-date "2024-${quarter}-01" \
    --end-date "2024-${quarter}-30" \
    --chunk-days 30 &
done
wait

# Phase 2: Transform Silver (1 min)
bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- --mode full

# Phase 3: Load Gold Dimensions (30 sec)
bash scripts/spark_submit.sh jobs/gold/load_dim_date.py -- --mode full &
bash scripts/spark_submit.sh jobs/gold/load_dim_time.py &
bash scripts/spark_submit.sh jobs/gold/load_dim_location.py &
bash scripts/spark_submit.sh jobs/gold/load_dim_pollutant.py &
wait

# Phase 4: Transform Gold Facts (2-3 min)
bash scripts/spark_submit.sh jobs/gold/transform_fact_hourly.py -- --mode full &
bash scripts/spark_submit.sh jobs/gold/transform_fact_daily.py -- --mode full &
bash scripts/spark_submit.sh jobs/gold/detect_episodes.py -- --mode full &
wait
```

### 🔄 Daily (Recurring, crontab)

```bash
# 02:00 - Bronze upsert
0 2 * * * cd /home/dlhnhom2/dlh-aqi && bash scripts/spark_submit.sh jobs/bronze/run_bronze_pipeline.py -- --mode upsert >> logs/bronze.log 2>&1

# 03:00 - Silver incremental
0 3 * * * cd /home/dlhnhom2/dlh-aqi && bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- --mode incremental >> logs/silver.log 2>&1

# 04:00 - Gold facts incremental (all parallel)
0 4 * * * cd /home/dlhnhom2/dlh-aqi && \
  (bash scripts/spark_submit.sh jobs/gold/transform_fact_hourly.py -- --mode incremental >> logs/gold_hourly.log 2>&1 & \
   bash scripts/spark_submit.sh jobs/gold/transform_fact_daily.py -- --mode incremental >> logs/gold_daily.log 2>&1 & \
   bash scripts/spark_submit.sh jobs/gold/detect_episodes.py -- --mode incremental >> logs/gold_episodes.log 2>&1 & \
   wait)
```

### 📊 Prefect Integration (Advanced)

```python
from Prefect.full_pipeline_flow import full_ingest_flow, bronze_ingest_task, silver_transform_task
from prefect import flow, task

@task(name="bronze_upsert", retries=2, retry_delay_seconds=300)
def bronze_task():
    result = bronze_ingest_task(mode="upsert")
    if not result["success"]:
        raise Exception(f"Bronze failed: {result['error']}")
    return result

@task(name="silver_incremental")  
def silver_task():
    result = silver_transform_task(mode="incremental")
    if not result["success"]:
        raise Exception(f"Silver failed: {result['error']}")
    return result

@task(name="gold_facts")
def gold_task():
    # Execute all 3 fact tables
    tasks = []
    # ... execute fact transforms ...
    return tasks

@flow(name="daily-aqi-pipeline")
def daily_pipeline():
    bronze = bronze_task()
    silver = silver_task()  # Only run if bronze success
    gold = gold_task()      # Only run if silver success
    return {"bronze": bronze, "silver": silver, "gold": gold}

if __name__ == "__main__":
    daily_pipeline.serve(
        name="daily-aqi-deployment",
        cron="0 2 * * *",
        tags=["production", "aqi", "incremental"]
    )
```

---

## ✅ Setup Checklist

- [ ] Install dependencies: `pip install -r requirements.txt`
- [ ] Create Iceberg schema: `bash scripts/spark_submit.sh scripts/create_lh_tables.py`
- [ ] Verify locations file: `hdfs dfs -ls /user/dlhnhom2/data/locations.jsonl`
- [ ] Test Bronze backfill (small range): `... --start-date 2024-01-01 --end-date 2024-01-07`
- [ ] Verify Bronze data: Query table in Spark SQL
- [ ] Run Silver full: `bash scripts/spark_submit.sh jobs/silver/run_silver_pipeline.py -- --mode full`
- [ ] Load Gold dimensions: Run all 4 `load_dim_*.py` scripts
- [ ] Transform Gold facts: Run all 3 `transform_fact_*.py` + `detect_episodes.py`
- [ ] Verify Gold data quality: Run monitoring queries
- [ ] Setup cron jobs for daily runs
- [ ] Setup Prefect flows (optional, for advanced scheduling)
- [ ] Monitor first week of logs for errors
- [ ] Document any customizations made to schema/parameters

---

## 📖 File Structure

```
jobs/
├── bronze/
│   └── run_bronze_pipeline.py          # Open-Meteo API → Bronze (403 lines)
├── silver/
│   └── run_silver_pipeline.py          # Bronze → Silver cleaning (313 lines)
└── gold/
    ├── load_dim_date.py                # Static date dimension
    ├── load_dim_time.py                # Static time dimension (24h)
    ├── load_dim_location.py            # Location metadata from JSONL
    ├── load_dim_pollutant.py           # Pollutant definitions from JSONL
    ├── transform_fact_hourly.py        # Hourly air quality enrichments
    ├── transform_fact_daily.py         # Daily aggregates & categories
    └── detect_episodes.py              # High AQI episode detection

scripts/
├── spark_submit.sh                     # YARN submission wrapper
├── create_lh_tables.py                 # Iceberg schema initialization
└── cleanup_spark_staging.sh            # Temp file cleanup

data/
├── locations.jsonl                     # Location metadata input
└── dim_pollutant.jsonl                 # Pollutant metadata input

docs/ingest/
├── README.md                           # Overview + quick start
├── bronze.md                           # Bronze layer details
├── silver.md                           # Silver layer details
└── gold.md                             # Gold layer details
```

---

## 📝 Important Notes

- ✅ **Idempotent**: Tất cả transforms dùng MERGE/upsert, safe để re-run
- ✅ **Scalable**: Hỗ trợ YARN dynamic allocation + Iceberg partitioning + parallel execution
- ✅ **Monitoring**: Mỗi script print metrics (records processed, time elapsed, errors)
- ✅ **Error handling**: Comprehensive exception handling + retry logic
- ✅ **Prefect-ready**: Hàm `execute_*()` trả về dict cho orchestration
- ✅ **Version control**: Commit khi thay đổi schema hoặc transform logic
- ✅ **Data quality**: Builtin deduplication + NULL handling + validation

---

**Last updated**: October 18, 2025
**Pipeline version**: 1.0 (Medallion Architecture)

