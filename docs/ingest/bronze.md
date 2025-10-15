# Tài liệu Ingest — Bronze

Tài liệu này mô tả cách hoạt động của bước Ingest (Bronze) trong pipeline AQI, cách chạy, và các cấu hình cần biết. Hiện tại tài liệu tập trung cho **bronze**; phần cho **silver/gold** sẽ thêm sau.

## Mục đích

Bronze layer chịu trách nhiệm lấy dữ liệu thô từ API (Open-Meteo Air Quality) và lưu xuống bảng Bronze (`hadoop_catalog.lh.bronze.open_meteo_hourly`) dưới dạng Iceberg table. Đây là lớp dữ liệu gần nguyên thủy, được chuẩn hóa cột, có thêm metadata `_ingested_at` và partition theo `date_utc`.

## File liên quan

- Code ingest chính: `ingest/bronze/ingest_bronze.py` (phiên bản đã tối ưu) 
- Script chạy wrapper: `scripts/spark_submit.sh` (dùng cho YARN)
- Locations input: `data/locations.jsonl`
- Schema: schema được định nghĩa trong `ingest/bronze/ingest_bronze.py` (BRONZE_SCHEMA)

## Yêu cầu (Prerequisites)

- Python 3.x trên driver node (cần `python3` khi chạy locally)
- Java và Spark (phiên bản tương thích với cluster)
- Các package Python: `openmeteo-requests`, `pandas`, `requests-cache`, `retry-requests`, `python-dotenv` (đã ghi trong file requirements.txt)
- HDFS / Iceberg catalog đã cấu hình: `hadoop_catalog.lh.bronze.open_meteo_hourly`

## Cấu hình môi trường

- Tập tin `.env` (nếu cần) ở project root để set biến môi trường (ví dụ WAREHOUSE_URI)
- `WAREHOUSE_URI` mặc định dùng `hdfs://khoa-master:9000/warehouse/iceberg` nếu không set

## Cách chạy

### Chạy local (Spark local mode)

```bash
# Upsert: tìm data mới nhất trong bronze, backfill từ đó đến hôm nay
python3 ingest/bronze/ingest_bronze.py --mode upsert

# Backfill: nạp lại dữ liệu lịch sử cho khoảng thời gian cụ thể
python3 ingest/bronze/ingest_bronze.py --mode backfill \
  --start-date 2024-01-01 \
  --end-date 2024-01-31

# Backfill với override (ghi đè dữ liệu cũ)
python3 ingest/bronze/ingest_bronze.py --mode backfill \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --override
```

### Chạy trên YARN (recommended for production)

**Lệnh cơ bản:**

```bash
# Upsert hàng ngày (chạy từ scheduler/cron)
spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.sql.adaptive.enabled=true \
  ingest/bronze/ingest_bronze.py --mode upsert

# Backfill lịch sử (chỉ chạy lần đầu hoặc khi cần nạp lại)
spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.sql.adaptive.enabled=true \
  ingest/bronze/ingest_bronze.py \
  --mode backfill \
  --start-date 2024-01-01 \
  --end-date 2024-12-31
```

**Lệnh với tuning tài nguyên:**

```bash
# Upsert với executor cấu hình cho production
spark-submit \
  --master yarn \
  --deploy-mode client \
  --num-executors 5 \
  --executor-memory 4G \
  --executor-cores 2 \
  --driver-memory 2G \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  ingest/bronze/ingest_bronze.py --mode upsert

# Backfill chunk nhỏ (30 ngày/chunk) cho dataset lớn
spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.maxExecutors=10 \
  ingest/bronze/ingest_bronze.py \
  --mode backfill \
  --start-date 2023-01-01 \
  --end-date 2023-12-31 \
  --chunk-days 30
```

**Sử dụng wrapper script:**

```bash
# Script wrapper tự động config YARN và optimize disk usage
bash scripts/spark_submit.sh -- ingest/bronze/ingest_bronze.py --mode upsert

# Xem command sẽ chạy (dry-run)
bash scripts/spark_submit.sh --dry-run -- ingest/bronze/ingest_bronze.py --mode upsert
```

**Ghi chú về deploy-mode:**
- `client`: Driver chạy trên node submit (dễ debug, xem log trực tiếp)
- `cluster`: Driver chạy trên YARN cluster (production, tự động failover)

Để chạy cluster mode:

```bash
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.dynamicAllocation.enabled=true \
  ingest/bronze/ingest_bronze.py --mode upsert
```

## Input locations

File `data/locations.jsonl` chứa danh sách location theo format JSONL, ví dụ:

```jsonl
{"location_key":"hanoi","location_name":"Hà Nội","latitude":21.0278,"longitude":105.8342,"timezone":"Asia/Ho_Chi_Minh"}
```

Các trường quan trọng: `location_key`, `location_name`, `latitude`, `longitude`. Nếu file có nhiều dòng, script sẽ lặp từng location.

## Schema (tóm tắt)

BRONZE_SCHEMA chứa các cột chính:
- location_key (string)
- ts_utc (timestamp)
- date_utc (date) — partition
- latitude, longitude
- aqi, aqi_pm25, aqi_pm10, ... (Int nullable)
- pm25, pm10, o3, no2, so2, co, aod, dust, uv_index, co2 (Double nullable)
- model_domain, request_timezone
- _ingested_at (timestamp)

Schema đầy đủ có trong `ingest/bronze/ingest_bronze.py`.

## Logic chính

### Mode: Backfill
1. Load danh sách locations từ `data/locations.jsonl`
2. Tách date range (`--start-date` đến `--end-date`) thành các chunk nhỏ (mặc định 90 ngày/chunk)
3. Với mỗi location và mỗi chunk:
   - Kiểm tra xem dữ liệu đã tồn tại chưa (trừ khi dùng `--override`)
   - Nếu chưa có: gọi Open-Meteo API lấy dữ liệu hourly
   - Chuẩn hóa: chuyển NaN → NULL, convert AQI columns sang Int64 nullable
   - Deduplicate trong pandas (drop_duplicates theo `location_key` + `ts_utc`)
   - Ghi vào Iceberg table với mode `append`
4. Sleep 1s giữa các API call để tránh rate limit

### Mode: Upsert (cập nhật hàng ngày)
1. Load danh sách locations
2. Với mỗi location:
   - **Tìm timestamp mới nhất** trong bảng bronze cho location đó
   - **Nếu không có data nào**: skip location này (thoát sớm), in log `No existing data in bronze, skipping upsert`
   - **Nếu có data**: tính toán khoảng thời gian cần backfill
     - Start date = ngày kế tiếp sau timestamp mới nhất
     - End date = hôm nay
     - Nếu đã up-to-date (latest >= today): skip
   - Gọi API và ghi data mới (không override, chỉ append)
3. Tóm tắt: hiển thị số location đã xử lý vs tổng số

**Tại sao upsert thiết kế như vậy?**
- Tránh duplicate: chỉ lấy data mới từ điểm đã có
- An toàn: không ghi đè data cũ, chỉ append
- Hiệu quả: skip location chưa có initial data (cần chạy backfill trước)
- Đơn giản: không cần tham số `--lookback-days` phức tạp

**Workflow khuyến nghị:**
1. Lần đầu: chạy `backfill` để nạp lịch sử (VD 6 tháng hoặc 1 năm)
2. Hàng ngày: chạy `upsert` từ scheduler (cron/Prefect) để cập nhật data mới

## Tích hợp với Prefect

Hàm `execute_ingestion(...)` trả về dict, dễ dàng tích hợp vào Prefect flow:

```python
{
  "success": True/False,
  "stats": {
    "total_rows": int, 
    "locations_processed": int  # chỉ có trong upsert mode
  },
  "elapsed_seconds": float,
  "error": "..." (nếu có)
}
```

### Ví dụ Prefect task đơn giản

```python
from prefect import task, flow
from ingest.bronze.ingest_bronze import execute_ingestion

@task(name="bronze_upsert", retries=2, retry_delay_seconds=300)
def bronze_upsert_task():
    """Daily upsert task for bronze layer"""
    result = execute_ingestion(
        mode="upsert",
        locations_path="data/locations.jsonl",
        table="hadoop_catalog.lh.bronze.open_meteo_hourly"
    )
    
    if not result["success"]:
        raise Exception(f"Bronze upsert failed: {result.get('error')}")
    
    print(f"Upsert completed: {result['stats']['total_rows']} rows")
    return result

@flow(name="daily_bronze_ingestion")
def daily_bronze_flow():
    bronze_result = bronze_upsert_task()
    return bronze_result

# Deploy to Prefect
if __name__ == "__main__":
    daily_bronze_flow.serve(
        name="daily-bronze-deployment",
        cron="0 2 * * *",  # Chạy lúc 2h sáng mỗi ngày
        tags=["bronze", "ingestion", "aqi"]
    )
```

### Ví dụ backfill một lần (ad-hoc)

```python
@task(name="bronze_backfill")
def bronze_backfill_task(start_date: str, end_date: str):
    """One-time backfill for historical data"""
    result = execute_ingestion(
        mode="backfill",
        locations_path="data/locations.jsonl",
        start_date=start_date,
        end_date=end_date,
        chunk_days=30,
        override=False,
        table="hadoop_catalog.lh.bronze.open_meteo_hourly"
    )
    
    if not result["success"]:
        raise Exception(f"Backfill failed: {result.get('error')}")
    
    return result

@flow(name="historical_backfill")
def backfill_flow(start: str, end: str):
    return bronze_backfill_task(start, end)

# Chạy một lần
backfill_flow("2024-01-01", "2024-12-31")
```

### Tips tích hợp Prefect

- **Spark session**: `execute_ingestion` tự tạo và đóng Spark session, không cần quản lý
- **Concurrency**: nếu chạy nhiều location song song, dùng Prefect concurrency limit để tránh quá tải cluster
- **Retry strategy**: đặt retry cho task (VD 2-3 lần, delay 5-10 phút) để xử lý lỗi tạm thời (network, API rate limit)
- **Monitoring**: log `stats` vào Prefect artifact hoặc external monitoring system
- **Resource tags**: tag task theo layer (bronze/silver/gold) để dễ quản lý flow

## Troubleshooting (vấn đề hay gặp)

- `Name node is in safe mode`: HDFS đang ở safe mode, dùng `hdfs dfsadmin -safemode leave` để tắt (hoặc chờ cluster ổn định)
- `Connection refused` khi Spark cố connect master: kiểm tra Spark master (yarn/resourcemanager) có sẵn
- `PLAN_VALIDATION_FAILED_RULE_IN_BATCH` hoặc lỗi liên quan optimizer khi dùng `dropDuplicates()` trên Spark+Iceberg: đã xử lý bằng cách dedupe ở pandas trước khi tạo Spark DataFrame
- Phiên bản thư viện: nếu gặp lỗi Avro/Iceberg, kiểm tra phiên bản Iceberg tương thích với Spark

## Best practices & Performance

- Chunk nhỏ (VD 30-90 ngày) để tránh timeouts và giảm memory pressure
- Dùng `requests_cache` + retry để giảm tải API
- Chỉ tạo SparkSession 1 lần cho cả run
- Dedupe ở pandas khi dataset cho phép (khi data size chunk nhỏ), nếu chunk lớn cân nhắc dùng Spark window + aggregation nhưng cẩn thận với Iceberg optimizer bug

## Next steps (Silver / Gold)

- Silver: chuyển đổi, enrichment (map pollutant codes, thêm quality flags), chuẩn hoá timezone, resample/aggregate hourly → hourly-clean
- Gold: chỉ số AQI tổng hợp, báo cáo, bảng sẵn sàng cho BI

Tôi sẽ tạo separate docs cho Silver/Gold khi bạn muốn — hiện tại đã hoàn thành tài liệu Bronze.
