# Hướng dẫn Ingest dữ liệu vào Bronze Layer

Tài liệu này hướng dẫn chi tiết quy trình ingest dữ liệu chất lượng không khí từ Open-Meteo API vào Bronze layer của data lakehouse.

## Tổng quan

Bronze layer là nơi lưu trữ dữ liệu thô (raw data) từ các nguồn bên ngoài. Dữ liệu được lưu trữ trong bảng Iceberg `hadoop_catalog.aq.bronze.raw_open_meteo_hourly` với cấu trúc (theo thứ tự và tên cột trong code):

- location_id STRING,        -- Chuỗi định danh địa điểm (ví dụ: "ha_noi")
- latitude DOUBLE,           -- Vĩ độ (decimal degrees)
- longitude DOUBLE,          -- Kinh độ (decimal degrees)
- ts TIMESTAMP,              -- Timestamp UTC của measurement
- aerosol_optical_depth DOUBLE, -- AOD (unitless)
- pm2_5 DOUBLE,              -- PM2.5 (µg/m³)
- pm10 DOUBLE,               -- PM10 (µg/m³)
- dust DOUBLE,               -- Dust (µg/m³)
- nitrogen_dioxide DOUBLE,   -- NO2 (µg/m³)
- ozone DOUBLE,              -- O3 (µg/m³)
- sulphur_dioxide DOUBLE,    -- SO2 (µg/m³)
- carbon_monoxide DOUBLE,    -- CO (µg/m³)
- uv_index DOUBLE,           -- UV index (unitless)
- uv_index_clear_sky DOUBLE, -- Clear-sky UV index (unitless)
- source STRING,             -- Nguồn dữ liệu ("open-meteo")
- run_id STRING,             -- UUID của lần chạy ingest
- ingested_at TIMESTAMP      -- Thời điểm ingest vào Bronze (UTC)
## Chuẩn bị môi trường

```bash
# Kiểm tra trạng thái safe mode
hdfs dfsadmin -safemode leave
```

```bash
# Spark Home (nếu cần)
export SPARK_HOME=${SPARK_HOME:-/home/dlhnhom2/spark}

# Warehouse URI (tùy chọn, mặc định là hdfs://khoa-master:9000/warehouse/iceberg)
export WAREHOUSE_URI=hdfs://khoa-master:9000/warehouse/iceberg
```

## Các tham số CLI

### Tham số bắt buộc

#### `--locations`
- **Type**: String (đường dẫn file)  
- **Required**: Có
- **Mô tả**: Đường dẫn tới file JSON chứa các địa điểm dưới dạng `{name: {"latitude": ..., "longitude": ...}}`
- **Ví dụ**: `configs/locations.json`

### Tham số thời gian

#### `--start-date` / `--start`
- **Type**: Date string (YYYY-MM-DD, UTC)
- **Required**: Bắt buộc nếu không dùng `--update-from-db`
- **Mô tả**: Ngày bắt đầu fetch dữ liệu (bao gồm)
- **Ví dụ**: `2024-01-01`

#### `--end-date` / `--end`  
- **Type**: Date string (YYYY-MM-DD, UTC)
- **Required**: Bắt buộc nếu không dùng `--update-from-db`
- **Mô tả**: Ngày kết thúc fetch dữ liệu (bao gồm)  
- **Ví dụ**: `2024-01-31`

### Tham số tùy chọn

#### `--chunk-days`
- **Type**: Integer
- **Default**: 10
- **Mô tả**: Chia range thành các chunk có độ dài ≤ N ngày cho mỗi lần gọi API (giảm kích thước payload và tránh timeout)

#### `--update-from-db`
- **Type**: Flag (boolean)
- **Default**: false
- **Mô tả**: Tự động tính `start_date` dựa trên `MAX(ts)` trong bảng Bronze và fetch từ thời điểm đó đến hiện tại. Nếu bảng rỗng, sẽ hỏi có backfill từ 2023-01-01 hay không.

#### `--yes`
- **Type**: Flag (boolean)  
- **Default**: false
- **Mô tả**: Tự động đồng ý các prompt (dùng cho môi trường non-interactive/CI). Khi dùng cùng `--update-from-db`, nếu bảng trống sẽ tự động backfill từ 2023-01-01 đến hiện tại.

#### `--mode`
- **Type**: Choice ["upsert", "replace-range"]
- **Default**: "upsert"
- **Mô tả**: 
  - `upsert`: Merge dữ liệu mới vào bảng (update nếu trùng key)
  - `replace-range`: Xóa dữ liệu trong range trước khi insert

Note: The ingest job reads locations from the JSON file you supply via `--locations`. To limit the run to a subset of locations, create a smaller JSON (for example `configs/locations_small.json`) containing only the entries you want and point `--locations` at it. See the example in the "Ingest chỉ một số địa điểm cụ thể" section.

## Các tình huống sử dụng

### 1. Ingest dữ liệu cho range thời gian cụ thể

```bash
# Ingest dữ liệu từ 2024-01-01 đến 2024-01-31, chia chunk 10 ngày (YARN)
# Uses the unified submit helper `scripts/run_spark.sh` which accepts --mode (yarn|standalone|local)
bash scripts/run_spark.sh --mode yarn jobs/bronze/open_meteo_bronze.py \
  --locations configs/locations.json \
  --start 2024-01-01 --end 2024-01-31 \
  --chunk-days 10
```

### 2. Update incremental từ database

```bash
# Update từ thời điểm mới nhất trong DB đến hiện tại (YARN)
# Nếu DB trống, sẽ hỏi có muốn backfill từ 2023-01-01 không
bash scripts/run_spark.sh --mode yarn jobs/bronze/open_meteo_bronze.py \
  --locations configs/locations.json \
  --update-from-db \
  --chunk-days 10
```

### 3. Backfill tự động (non-interactive)

```bash
# Backfill tự động từ 2023-01-01 nếu DB trống, hoặc update từ MAX(ts) (YARN)
bash scripts/run_spark.sh --mode yarn jobs/bronze/open_meteo_bronze.py \
  --locations configs/locations.json \
  --update-from-db \
  --yes \
  --chunk-days 10
```

Note: The ingest job processes and MERGE each API chunk independently to reduce driver memory usage. Set `--chunk-days` smaller for very large backfills to limit per-chunk size.

### 4. Ingest chỉ một số địa điểm cụ thể

To limit which locations are ingested, edit `configs/locations.json` to include only the locations you want to run for this job. The ingest job reads locations from that file and processes them in order.

Example: create a small `configs/locations_small.json` containing only the desired entries and point the job to it:

```bash
# Ingest only a reduced set of locations defined in configs/locations_small.json (local fast loop)
bash scripts/run_spark.sh --mode local jobs/bronze/open_meteo_bronze.py \
  --locations configs/locations_small.json \
  --start 2024-01-01 --end 2024-01-31 \
  --chunk-days 10
```

### 5. Replace range (ingest lại dữ liệu)

```bash
# Xóa và ingest lại dữ liệu trong range (YARN)
bash scripts/run_spark.sh --mode yarn jobs/bronze/open_meteo_bronze.py \
    --locations configs/locations.json \
  --start 2024-01-01 \
  --end 2024-01-07 \
    --mode replace-range
```

## Kiểm tra dữ liệu trong Bronze

### 1. Kết nối Spark SQL

```bash
 SPARK_HOME=${SPARK_HOME:-/home/dlhnhom2/spark}
$SPARK_HOME/bin/spark-sql --master local[1] \
  --conf spark.sql.catalogImplementation=in-memory \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop \
  --conf spark.sql.catalog.hadoop_catalog.warehouse=hdfs://khoa-master:9000/warehouse/iceberg

```

### 2. Kiểm tra tables

```sql
-- Xem các bảng trong namespace aq
SHOW TABLES IN hadoop_catalog.aq;
```

### 3. Kiểm tra tổng quan dữ liệu

```sql
-- Đếm tổng số rows
SELECT COUNT(*) AS total_rows FROM hadoop_catalog.aq.bronze.raw_open_meteo_hourly;

-- Lấy sample dữ liệu mới nhất
SELECT * FROM hadoop_catalog.aq.bronze.raw_open_meteo_hourly 
ORDER BY ts DESC LIMIT 20;

-- Kiểm tra time range tổng thể
SELECT MIN(ts) AS min_ts, MAX(ts) AS max_ts 
FROM hadoop_catalog.aq.bronze.raw_open_meteo_hourly;
```

### 4. Phân tích dữ liệu theo địa điểm

```sql
-- Số lượng records theo địa điểm
SELECT location_id, COUNT(*) AS total_records
FROM hadoop_catalog.aq.bronze.raw_open_meteo_hourly
GROUP BY location_id
ORDER BY total_records DESC;

-- Time range theo từng địa điểm  
SELECT location_id, MIN(ts) AS min_ts, MAX(ts) AS max_ts
FROM hadoop_catalog.aq.bronze.raw_open_meteo_hourly
GROUP BY location_id
ORDER BY location_id;
```

### 5. Kiểm tra data quality

```sql
-- Tìm duplicates theo (ts, location_id)
WITH dup AS (
  SELECT ts, location_id, COUNT(*) AS cnt
  FROM hadoop_catalog.aq.bronze.raw_open_meteo_hourly
  GROUP BY ts, location_id
  HAVING COUNT(*) > 1
)
SELECT t.*
FROM hadoop_catalog.aq.bronze.raw_open_meteo_hourly t
JOIN dup d ON t.ts = d.ts AND t.location_id = d.location_id
ORDER BY t.location_id, t.ts;

-- Kiểm tra null values
SELECT 
  COUNT(*) AS total_rows,
  COUNT(pm2_5) AS pm2_5_non_null,
  COUNT(pm10) AS pm10_non_null,
  COUNT(nitrogen_dioxide) AS no2_non_null,
  COUNT(ozone) AS o3_non_null
FROM hadoop_catalog.aq.bronze.raw_open_meteo_hourly;
```

## Xử lý dữ liệu và maintenance

### 1. Xóa dữ liệu (khi cần ingest lại)

```sql
-- ⚠️ CHỈ THỰC HIỆN KHI MUỐN INGEST LẠI DATA
-- Truncate toàn bộ bảng
TRUNCATE TABLE hadoop_catalog.aq.bronze.raw_open_meteo_hourly;

-- Hoặc xóa theo điều kiện
DELETE FROM hadoop_catalog.aq.bronze.raw_open_meteo_hourly 
WHERE ts >= '2024-01-01' AND ts <= '2024-01-31';
```

### 2. Iceberg maintenance

Các thao tác maintenance được tự động chạy sau mỗi lần ingest:

```sql
-- Rewrite data files để tối ưu kích thước
CALL hadoop_catalog.system.rewrite_data_files(
  'aq.bronze.raw_open_meteo_hourly',
  map('target-file-size-bytes', CAST(134217728 AS bigint))
);

-- Expire old snapshots (giữ 30 ngày gần nhất)
CALL hadoop_catalog.system.expire_snapshots(
  'aq.bronze.raw_open_meteo_hourly',
  CURRENT_TIMESTAMP - INTERVAL 30 DAYS
);

-- Xóa orphan files
CALL hadoop_catalog.system.remove_orphan_files('aq.bronze.raw_open_meteo_hourly');
```

## Troubleshooting

### Lỗi thường gặp

#### 1. Safe Mode Error
```
ERROR: Name node is in safe mode
```
**Giải pháp**: Tắt safe mode như hướng dẫn ở phần chuẩn bị.

#### 2. HDFS Connection Error
```
ERROR: Failed to connect to hdfs://khoa-master:9000
```
**Giải pháp**: Kiểm tra kết nối mạng và cấu hình Hadoop client.

#### 3. API Rate Limiting
```
ERROR: Too many requests to Open-Meteo API
```
**Giải pháp**: Tăng `--chunk-days` để giảm số lần gọi API, hoặc chờ và thử lại.

#### 4. Memory Error khi ingest large range
**Giải pháp**: 
- Giảm `--chunk-days` xuống 5 hoặc 7
- Chia nhỏ range thời gian thành nhiều lần chạy

#### 5. Duplicate Key Error
```
ERROR: Duplicate key violations during MERGE
```
**Giải pháp**: Sử dụng `--mode replace-range` để xóa và ingest lại.

### Logs và debugging

- Job logs được lưu trong YARN: `yarn logs -applicationId <app_id>`
- Local cache của API requests: `.cache/` directory  
- Spark staging: `/user/<username>/.sparkStaging/`

### Recovery procedures

#### Khôi phục từ failed run:
1. Kiểm tra logs để xác định nguyên nhân
2. Sử dụng `--mode replace-range` để ingest lại range bị lỗi
3. Chạy data quality checks sau khi ingest thành công

#### Cleanup môi trường:
```bash
# Xóa Spark staging files
hdfs dfs -rm -r /user/$USER/.sparkStaging/*

# Xóa local cache
rm -rf .cache/

# Xóa local spark warehouse
rm -rf spark-warehouse/
```

## Performance tuning

### Tối ưu hóa ingest
- **chunk-days**: 7-14 ngày cho historical data, 1-3 ngày cho incremental
- **Concurrent locations**: Script chạy tuần tự theo location để tránh overload API
- **Caching**: HTTP requests được cache 1 giờ để tránh duplicate calls

### Tối ưu hóa Iceberg
- **Partitioning**: Bảng được partition theo `days(ts)` 
- **File size**: Target 128MB per file
- **Compaction**: Tự động chạy sau mỗi ingest

Note: Some Silver-layer tables (notably `aq.silver.aq_components_hourly`) were adjusted in code to target smaller Parquet files (32MB) and the components job now repartitions the DataFrame before performing the Iceberg MERGE to avoid single-task driver writes during local runs. If you experience OOM during Iceberg writes, consider increasing `SPARK_DRIVER_MEMORY` and `SPARK_SQL_SHUFFLE_PARTITIONS` in `.env` or run the job on a Yarn/standalone cluster.

## Quy trình dọn dẹp hệ thống (WSL2/Docker Desktop)

Khi cần thu hồi disk space sau khi xử lý large datasets:

```bash
# 1. Xóa temp files và logs
sudo rm -rf /tmp/* /var/tmp/* \
             /var/log/hadoop-yarn/containers/* \
             /var/hadoop/yarn/local/usercache/*/*

# 2. Trim filesystem
sudo fstrim -av
sudo dd if=/dev/zero of=~/zero.fill bs=4M iflag=fullblock oflag=direct status=progress

# 3. Sync và cleanup
sync
sudo rm -f ~/zero.fill
```

Sau đó shutdown WSL và optimize VHDX files:
```powershell
# Trong PowerShell (Windows)
wsl --shutdown

# Optimize Docker Desktop VHDX files
Optimize-VHD -Path "E:\Docker\DockerDesktopWSL\main\ext4.vhdx" -Mode Full
Optimize-VHD -Path "E:\Docker\DockerDesktopWSL\disk\docker_data.vhdx" -Mode Full
```