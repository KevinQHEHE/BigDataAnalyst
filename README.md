# AQ Lakehouse — Vietnam Air Quality Data Pipeline

Pipeline thu thập dữ liệu chất lượng không khí theo giờ từ Open-Meteo API, lưu trữ trong bảng Bronze layer dựa trên Iceberg, và hỗ trợ quản lý dữ liệu với các tính năng idempotent và range replacement.

## Kiến trúc hệ thống

### Data Layer
- **Bronze Layer** (`hadoop_catalog.aq.bronze.raw_open_meteo_hourly`) – Bảng Iceberg lưu trữ dữ liệu thô từ Open-Meteo API, với key là `(location_id, ts)`. Dữ liệu được ghi thông qua `MERGE` operation với UUID và timestamp để truy xuất lineage.
- **Silver Layer** – Các bảng được chuẩn hoá để phục vụ tính toán chỉ số:
  - `aq.silver.air_quality_hourly_clean`: đổi tên cột, chuẩn hoá `ts_utc`, thêm cờ chất lượng (`valid_flags`).
  - `aq.silver.aq_components_hourly`: tính rolling components (24h trung bình cho PM, 8h max cho O₃/CO, 1h max cho NO₂/SO₂) cùng cờ `component_valid_flags`.
  - `aq.silver.aq_index_hourly`: tính AQI tổng hợp, AQI theo pollutant, pollutant chi phối và nhãn category.

### Các thành phần chính
- **Ingest Job** (`jobs/ingest/open_meteo_bronze.py`) – Thu thập dữ liệu từ API và ghi vào Bronze
- **Silver Clean Job** (`jobs/silver/clean_hourly.py`) – Lấy dữ liệu Bronze, chuẩn hoá schema & metadata và ghi vào `aq.silver.air_quality_hourly_clean`
- **Silver Clean Job chi tiết**:
  1. Đọc tham số `--start/--end` và chế độ ghi `--mode` (merge hoặc replace). The ingest job reads locations from the JSON file passed with `--locations`; to limit locations, edit that file.
  2. Khởi tạo Spark session với Iceberg, đảm bảo bảng `aq.silver.air_quality_hourly_clean` tồn tại (schema có `ts_utc`, `date_utc`, các cột pollutant, `valid_flags`, metadata) và partition theo `(location_id, days(ts_utc))`.
  3. Nạp dữ liệu từ `aq.raw_open_meteo_hourly` trong khoảng thời gian yêu cầu; nếu không có bản ghi thì kết thúc run.
  4. Chuẩn hoá dữ liệu:
     - Đổi tên cột (`pm2_5 → pm25`, `aerosol_optical_depth → aod`, …) và chuẩn timestamp `ts → ts_utc` (UTC).
     - Sinh thêm `date_utc`, `run_id`, `ingested_at`, `notes`, đồng thời giữ lại `bronze_run_id/bronze_ingested_at` để truy xuất lineage.
     - Tạo `valid_flags` (map boolean) cho từng chỉ số (`pm25_nonneg`, `o3_nonneg`, …) và thông tin hiện diện `ts/latitude/longitude`.
  5. Nếu chạy ở chế độ `--mode replace`, xóa trước các bản ghi Silver nằm trong khung thời gian tương ứng.
  6. Ghi vào bảng Silver bằng Iceberg `MERGE INTO`: nếu key `(location_id, ts_utc)` đã tồn tại thì cập nhật, ngược lại chèn mới.
  7. In thống kê `MIN/MAX/COUNT` theo `location_id` và `RUN_ID` để theo dõi việc ingest.
- **Silver Components Job** (`jobs/silver/components_hourly.py`) – Tính toán rolling components cần cho AQI, ghi vào `aq.silver.aq_components_hourly`
- **Silver Components Job chi tiết**:
  1. Nhận tham số `--start/--end`, optional `--location-id`, chế độ ghi `--mode` và `--calc-method`. Xác định khoảng giờ sẽ tính và ghi nhận tên phương pháp (mặc định `simple_rolling_v1`).
  2. Khởi tạo Spark session, đảm bảo bảng `aq.silver.aq_components_hourly` tồn tại với schema: các cột rolling (`pm25_24h_avg`, `pm10_24h_avg`, `o3_8h_max`, `co_8h_max`, `no2_1h_max`, `so2_1h_max`), `component_valid_flags`, `calc_method`, `run_id`, `computed_at`, partition `(location_id, days(ts_utc))`.
  3. Đọc bảng clean `aq.silver.air_quality_hourly_clean` với window mở rộng để đủ dữ liệu quá khứ (ví dụ 24h cho PM, 8h cho O₃/CO). Nếu không có dữ liệu thì dừng.
  4. Với từng location, tạo Window functions:
     - 24h trailing average cho PM2.5/PM10, 8h trailing max cho O₃/CO.
     - Đếm số giờ hợp lệ trong từng window và kiểm tra nhịp 1 giờ (`continuous_hour`).
  5. Sinh các cột kết quả: chỉ tính giá trị rolling khi đủ dữ liệu (`>=18` giờ cho PM, `>=6` giờ cho O₃/CO); NO₂/SO₂ lấy giá trị giờ hiện tại. Đồng thời tạo `component_valid_flags` (ví dụ `pm25_24h_sufficient`, `o3_8h_sufficient`, `no2_present`, …), `calc_method`, `run_id`, `computed_at`, `date_utc`.
  6. Giới hạn DataFrame về đúng khoảng `[start, end]`. Nếu không còn bản ghi thì kết thúc.
  7. Với `--mode replace`, xóa trước các record trong khoảng target; sau đó dùng Iceberg `MERGE INTO` để upsert theo key `(location_id, ts_utc)`.
  8. In thống kê `MIN/MAX/COUNT` theo `location_id` và `RUN_ID` để theo dõi kết quả tính toán.
- **Silver AQI Job** (`jobs/silver/index_hourly.py`) – Tính AQI tổng hợp & pollutant chi phối, ghi vào `aq.silver.aq_index_hourly`
- **Silver AQI Job chi tiết**:
  1. Đọc tham số `--start/--end`, optional `--location-id`, chế độ ghi `--mode` và tên phương pháp `--calc-method` (mặc định `epa_like_v1`).
  2. Khởi tạo Spark session, đảm bảo bảng `aq.silver.aq_index_hourly` tồn tại với schema gồm `aqi`, `category`, `dominant_pollutant`, từng AQI theo pollutant và metadata (`calc_method`, `run_id`, `computed_at`), partition `(location_id, days(ts_utc))`.
  3. Nạp bảng components `aq.silver.aq_components_hourly` trong khoảng thời gian yêu cầu; nếu chưa có dữ liệu thì dừng run.
  4. Nội suy AQI theo chuẩn EPA:
     - Chuyển đổi đơn vị (µg/m³ → ppm/ppb) bằng molar mass và thể tích molar.
     - Sử dụng bảng breakpoints để tính AQI PM₂.₅, PM₁₀, O₃, NO₂, SO₂, CO.
  5. Lấy AQI tổng bằng giá trị lớn nhất trong các AQI thành phần; ánh xạ sang category (“Good”, “Moderate”, …); xác định pollutant chi phối (pollutant cho AQI lớn nhất).
  6. Ghi metadata (`calc_method`, `run_id`, `computed_at`, `date_utc`) và chọn cột theo schema output.
  7. Nếu `--mode replace` thì xóa các bản ghi trong khoảng target trước khi ghi; sau đó dùng `MERGE INTO` upsert theo `(location_id, ts_utc)`.
  8. In thống kê min/max/count theo `location_id` và `RUN_ID` để theo dõi kết quả.
- **Spark Session Builder** (`src/aq_lakehouse/spark_session.py`) – Cấu hình Spark với Iceberg catalogs
- **Submit Script** (`scripts/submit_yarn.sh`) – Helper script để submit PySpark jobs lên YARN
- **Location Config** (`configs/locations.json`) – Định nghĩa các địa điểm và tọa độ
- **Validation Notebook** (`notebooks/silver_validation.ipynb`) – Kiểm tra counts, null-rate và sample windows cho các bảng Silver

## Cài đặt nhanh

### 1. Cài đặt dependencies
Yêu cầu Python 3.10+:
```bash
pip install -r requirements.txt
```

### 2. Kiểm tra môi trường
Đảm bảo Spark và Hadoop clients có thể truy cập `hdfs://khoa-master:9000/` và các Iceberg extensions khả dụng trên cluster:

```bash
# Kiểm tra HDFS connection
hdfs dfs -ls hdfs://khoa-master:9000/

# Tắt safe mode nếu cần
hdfs dfsadmin -safemode leave
```

### 3. Chạy ingest đơn giản
```bash
# Ingest dữ liệu từ 2024-01-01 đến 2024-01-31
bash scripts/submit_yarn.sh ingest/open_meteo_bronze.py \
    --locations configs/locations.json \
  --start 2024-01-01 --end 2024-01-31 \
  --chunk-days 10
```

### 3b. Xây dựng bảng Silver cho cùng khoảng thời gian
```bash
# Chuẩn hoá Bronze -> Silver clean
bash scripts/submit_yarn.sh silver/clean_hourly.py \
    --start 2024-01-01T00:00:00 \
    --end   2024-01-31T23:00:00

# Tính rolling components
bash scripts/submit_yarn.sh silver/components_hourly.py \
    --start 2024-01-01T00:00:00 \
    --end   2024-01-31T23:00:00

# Tính AQI hourly
bash scripts/submit_yarn.sh silver/index_hourly.py \
    --start 2024-01-01T00:00:00 \
    --end   2024-01-31T23:00:00
```

### 4. Update incremental từ database hiện tại
```bash
# Tự động detect từ MAX(ts) và update đến hiện tại
bash scripts/submit_yarn.sh ingest/open_meteo_bronze.py \
    --locations configs/locations.json \
    --update-from-db \
    --yes \
  --chunk-days 10

Note: The ingest job was refactored to stream and merge each API chunk independently. This keeps driver memory usage low and lets large backfills run without accumulating all rows in-memory before writing.
```

## Environment Variables

### Các biến môi trường chính
- `SPARK_HOME` — Đường dẫn đến Spark installation (mặc định: `/home/dlhnhom2/spark`)
- `WAREHOUSE_URI` — URI của Iceberg warehouse (mặc định: `hdfs://khoa-master:9000/warehouse/iceberg`)
- `SPARK_SUBMIT` — Đường dẫn custom đến spark-submit binary (tùy chọn)

## Chi tiết Ingest Job

### Data Schema
Bảng Bronze lưu trữ các trường sau:
```sql
location_id STRING,           -- Tên địa điểm từ locations.json
latitude DOUBLE,             -- Vĩ độ  
longitude DOUBLE,            -- Kinh độ
ts TIMESTAMP,                -- Timestamp UTC
aerosol_optical_depth DOUBLE, -- Độ sâu quang học aerosol  
pm2_5 DOUBLE,               -- PM2.5 (μg/m³)
pm10 DOUBLE,                -- PM10 (μg/m³)
dust DOUBLE,                -- Dust (μg/m³)
nitrogen_dioxide DOUBLE,     -- NO2 (μg/m³)
ozone DOUBLE,               -- O3 (μg/m³)
sulphur_dioxide DOUBLE,     -- SO2 (μg/m³)
carbon_monoxide DOUBLE,     -- CO (mg/m³)
uv_index DOUBLE,            -- UV Index
uv_index_clear_sky DOUBLE,  -- UV Index clear sky
source STRING,              -- "open_meteo"
run_id STRING,              -- UUID của run
ingested_at TIMESTAMP       -- Thời gian ingest
```

### Tính năng chính
- **HTTP Caching**: Cached session với 1-hour expiry để tránh duplicate API calls
- **Retry Logic**: 5 lần retry với exponential backoff
- **Data Sanitization**: Chuyển đổi negative readings thành `NULL`
- **Deduplication**: Loại bỏ duplicate `(location_id, ts)` trong cùng batch
- **Idempotent Writes**: Sử dụng Iceberg `MERGE` để update thay vì duplicate
- **Range Replacement**: Support xóa và ingest lại data cho range cụ thể
- **Auto Maintenance**: Tự động chạy compaction và cleanup sau mỗi ingest

### Các chế độ ingest
- **Upsert** (mặc định): Merge data mới, update nếu trùng key
- **Replace Range**: Xóa data trong range trước khi insert

## Cấu trúc dữ liệu

| Component | Schema | Partitioning | Đặc điểm |
|-----------|--------|-------------|----------|
| Bronze | Hourly measurements với metadata | `days(ts)` | Iceberg Format v2, hash distribution, target file size 128MB |

## Data Quality & Maintenance

### Tự động maintenance sau mỗi ingest
- **File Compaction**: `rewrite_data_files` để tối ưu file sizes
- **Snapshot Cleanup**: `expire_snapshots` giữ lại 30 ngày gần nhất  
- **Orphan Cleanup**: `remove_orphan_files` xóa files không sử dụng

### Spark configuration
- Force `spark.sql.catalogImplementation=in-memory` để tránh Derby metastore locks
- Disable Arrow execution (`spark.sql.execution.arrow.pyspark.enabled=false`)
- Pin dependencies: `numpy==1.26.4`, `pyarrow==14.0.2` để tránh compatibility issues

## Cấu trúc project

```
configs/locations.json              # Danh sách địa điểm và tọa độ
jobs/ingest/open_meteo_bronze.py   # Logic ingest Bronze (MERGE + maintenance)
jobs/silver/clean_hourly.py        # Bronze -> Silver clean table
jobs/silver/components_hourly.py   # Silver clean -> components (rolling)
jobs/silver/index_hourly.py        # Silver components -> AQI index
src/aq_lakehouse/spark_session.py # Spark session builder với Iceberg config  
scripts/submit_yarn.sh             # Helper submit PySpark jobs to YARN
notebooks/silver_validation.ipynb # Notebook validation counts/null rates sample windows
docs/ingest_bronze.md              # Hướng dẫn chi tiết ingest Bronze
requirements.txt                   # Python dependencies
```

## Sử dụng nâng cao

### 1. Ingest specific locations
To limit which locations are ingested, create a reduced `configs/locations.json` containing only the entries you want (for example `configs/locations_small.json`) and point the ingest job at that file:

```bash
bash scripts/submit_yarn.sh ingest/open_meteo_bronze.py \
  --locations configs/locations_small.json \
  --start 2024-01-01 --end 2024-01-31 \
  --chunk-days 10
```

### 2. Replace range (ingest lại data)
```bash
bash scripts/submit_yarn.sh ingest/open_meteo_bronze.py \
  --locations configs/locations.json \
  --start 2024-01-01 \
  --end 2024-01-07 \
  --mode replace-range
```

### 3. Kiểm tra dữ liệu với Spark SQL
```bash
# Kết nối Spark SQL
 SPARK_HOME=${SPARK_HOME:-/home/dlhnhom2/spark}
$SPARK_HOME/bin/spark-sql --master local[1] \
  --conf spark.sql.catalogImplementation=in-memory \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop \
  --conf spark.sql.catalog.hadoop_catalog.warehouse=hdfs://khoa-master:9000/warehouse/iceberg

```

Queries phổ biến:
```sql
-- Kiểm tra tổng quan
SELECT COUNT(*) FROM hadoop_catalog.aq.bronze.raw_open_meteo_hourly;

-- Time range
SELECT MIN(ts) AS min_ts, MAX(ts) AS max_ts 
FROM hadoop_catalog.aq.bronze.raw_open_meteo_hourly;

-- Phân bố theo địa điểm
SELECT location_id, COUNT(*) AS records
FROM hadoop_catalog.aq.raw_open_meteo_hourly
GROUP BY location_id ORDER BY records DESC;
```

## Mở rộng pipeline

### 1. Thêm địa điểm mới
Cập nhật `configs/locations.json`:
```json
{
    "Hà Nội": { "latitude": 21.028511, "longitude": 105.804817 },
    "TP. Hồ Chí Minh": { "latitude": 10.762622, "longitude": 106.660172 },
    "Đà Nẵng": { "latitude": 16.054406, "longitude": 108.202167 },
    "Cần Thơ": { "latitude": 10.045, "longitude": 105.747 }
}
```

### 2. Thêm measurements mới
Cập nhật `HOURLY_VARS` trong `jobs/ingest/open_meteo_bronze.py` và schema của bảng Bronze.

### 3. Scheduling
Wrap `scripts/submit_yarn.sh` trong cron hoặc Airflow:
```bash
# Cron example: chạy hàng giờ
0 * * * * cd /path/to/dlh-aqi && bash scripts/submit_yarn.sh ingest/open_meteo_bronze.py --locations configs/locations.json --update-from-db --yes --chunk-days 1
```

## Troubleshooting

### Lỗi thường gặp

#### HDFS Safe Mode
```bash
hdfs dfsadmin -safemode leave
```

#### Connection issues
- Kiểm tra network connectivity đến `khoa-master:9000`
- Verify Hadoop client configuration

#### API Rate Limiting  
- Tăng `--chunk-days` để giảm frequency
- Sử dụng `time.sleep(0.2)` trong code để pacing

#### Memory errors với large ranges
- Giảm `--chunk-days` xuống 5-7
- Chia nhỏ range thành multiple runs

### Logs và debugging
- **YARN logs**: `yarn logs -applicationId <app_id>`
- **Local cache**: `.cache/` directory
- **Spark staging**: `/user/<username>/.sparkStaging/`

### Recovery procedures
1. Kiểm tra logs để tìm root cause
2. Sử dụng `--mode replace-range` để re-ingest failed range
3. Validate data quality sau khi recovery

### Cleanup commands
```bash
# Clear Spark staging
hdfs dfs -rm -r /user/$USER/.sparkStaging/*

# Clear local cache  
rm -rf .cache/ spark-warehouse/

# System cleanup (WSL2/Docker)
sudo rm -rf /tmp/* /var/tmp/*
sudo fstrim -av
```

## Performance Notes

### API Optimization
- **Chunking**: 7-14 ngày cho historical, 1-3 ngày cho incremental
- **Caching**: 1-hour HTTP cache để tránh redundant calls
- **Rate limiting**: 0.2s delay giữa các API calls

### Iceberg Optimization  
- **Partitioning**: `days(ts)` cho efficient time-based queries
- **File sizing**: Target 128MB files cho optimal scan performance
- **Compaction**: Auto-compaction sau mỗi ingest để tối ưu file layout

Để biết thêm chi tiết về ingest process, xem `docs/ingest_bronze.md`.

### Environment knobs

- `START` / `END` — inclusive UTC dates for ingestion.
- `MODE` — `upsert` (default) or `replace-range` (pre-delete range before merge).
- `LOCATION_IDS` — optional space-separated list to ingest/delete only those locations.
- `FULL` — `true` triggers a Gold truncate + rebuild from Silver.
- `LOCATIONS_FILE` — override `configs/locations.json` if you maintain multiple location sets.
- `CHUNK_DAYS` — adjust API chunking window (default `10`).

The script captures the `RUN_ID` emitted by the ingest job and passes it to the Gold merge so only impacted partitions are recomputed.

## Ingestion job details

`jobs/ingest/open_meteo_bronze.py` handles API access and Bronze writes (always submit via Spark/YARN using `script/submit_yarn.sh`).

- Reads coordinates from `configs/locations.json` (keys are the `location_id`).
- Uses a cached, retried Open-Meteo client with polite pacing (`time.sleep(0.2)` per API call).
- Sanitises negative pollutant readings to `NULL` before writing.
- Drops duplicate `(location_id, ts)` pairs within the fetched batch.
- Writes through Iceberg `MERGE` so reruns update existing rows instead of duplicating them.
- Prints `RUN_ID=<uuid>` on success and automatically runs Iceberg maintenance procedures.

Range replacement is achieved with `--mode replace-range` which issues an Iceberg `DELETE` for the requested window and location set before the merge. For incremental catch-up runs, use `--update-from-db` to start from the next hour after the current `MAX(ts)`.

## Data layers

| Layer | Contract | Partitioning | Notes |
| --- | --- | --- | --- |
| Bronze | hourly metrics, lat/lon, `run_id`, `ingested_at` | `days(ts)` | Format v2, hash distribution, 128 MB target files |
| Silver | view fields: `location_id`, `ts_utc`, `date`, pollutant columns, `lat`, `lon`, `run_id`, `ingested_at` | n/a | exposes Bronze data cleaned and ready for joins |
| Gold | daily averages: `pm25_avg_24h`, `pm10_avg_24h`, `no2_avg_24h`, `o3_avg_24h`, `so2_avg_24h`, `co_avg_24h`, `uv_index_avg`, `uv_index_cs_avg`, `aod_avg` | `(date, location_id)` | Upsert per `RUN_ID`; rebuildable in one pass |

Adding new KPIs (e.g., AQI calculations, rolling windows) follows the same pattern—derive from Silver keyed by date/location and merge only the changed slices.

## Data quality and housekeeping

- The pipeline runs null, duplication, and pollutant range checks on the Silver view after each execution. Any non-zero count aborts the run.
- Bronze maintenance (`rewrite_data_files`, `expire_snapshots`, `remove_orphan_files`) is executed post-ingest to keep storage tidy.
- Gold maintenance and Spark staging cleanup run at the end of every pipeline invocation (purges `/user/<user>/.sparkStaging`, prunes `./spark-warehouse`, and trims stale HTTP caches).
- All Spark entrypoints force `spark.sql.catalogImplementation=in-memory` to avoid local Derby metastore locks when multiple jobs run on the same host.
- Dependencies pin `numpy==1.26.4` and `pyarrow==14.0.2` to avoid known Arrow/PySpark compatibility issues. Arrow execution is disabled by default (`spark.sql.execution.arrow.pyspark.enabled=false`).

## Project layout

```
jobs/ingest/open_meteo_bronze.py   # Bronze ingestion logic (MERGE + housekeeping)
src/aq_lakehouse/spark_session.py  # Spark builder with Iceberg catalog config
script/submit_yarn.sh              # Helper to submit arbitrary PySpark jobs to YARN
scripts/run_pipeline.sh            # One-command pipeline orchestrator (spark-submit + housekeeping)
scripts/reset_pipeline_state.sh    # Truncate/drop all layers and clear staging/cache files
docs/ingest_bronze.md              # Detailed operations guide (this README's companion)
configs/locations.json             # Location IDs and coordinates
```

## Extending the pipeline

1. **Add locations** – update `configs/locations.json`. The next pipeline run will merge new coordinates into `dim_locations` automatically.
2. **Add measures** – include new hourly fields in `HOURLY_VARS` and extend Bronze/Silver/Gold transformations accordingly.
3. **New Gold metrics** – create another aggregation CTE inside the Gold update block or a separate Iceberg table merged by `RUN_ID`.
4. **Scheduling** – wrap `scripts/run_pipeline.sh` inside cron, Airflow, or another orchestrator. Non-zero exits provide clear failure signals. Set `EXPIRE_SNAPSHOTS=true` if you want the run to call Iceberg’s snapshot expiration procedure at the end.
5. **Cold rebuilds** – run `scripts/reset_pipeline_state.sh` before a historical reload to ensure a clean slate (leave HDFS safe mode first if it is active). Snapshot expiration is skipped by default during the reset; enable it via `RESET_EXPIRE_SNAPSHOTS=true` when needed, then start the pipeline with `FULL=true`.

## Troubleshooting

- **Authentication to HDFS fails**: verify Hadoop configs on the driver and ensure the warehouse path is reachable (`hdfs dfs -ls hdfs://khoa-master:9000/warehouse/iceberg`).
- **`RUN_ID` missing in orchestrator logs**: ensure stdout is not suppressed; the string is printed on the final line by the ingest job.
- **`spark-sql` not on PATH**: point `SPARK_SQL` to the desired binary, e.g. `SPARK_SQL=/opt/spark/bin/spark-sql ./scripts/run_pipeline.sh`.
- **Residual data after a failed run**: execute `./scripts/reset_pipeline_state.sh` to truncate tables, drop views, and clear Spark staging directories before retrying. The reset also removes any local Derby metadata remnants.
- **Replaying a specific period**: set `MODE=replace-range` along with the appropriate `START`/`END` and (optionally) `LOCATION_IDS`. The Gold layer will automatically resynchronise the affected days.

For deeper operational steps, consult `docs/ingest_bronze.md`.
