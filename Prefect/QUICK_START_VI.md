# Hướng Dẫn Nhanh - Prefect Flows

## 🎯 Tóm Tắt

Hệ thống Prefect flows đã được triển khai đầy đủ cho pipeline DLH-AQI với:
- ✅ Chạy trên YARN thông qua `spark_submit.sh`
- ✅ Một SparkSession cho mỗi flow (không tạo nhiều session)
- ✅ Lịch chạy tự động mỗi giờ
- ✅ Backfill dữ liệu lịch sử với chunking
- ✅ Tài liệu đầy đủ

## 🚀 Cài Đặt Nhanh

### Bước 1: Deploy flows

```bash
python Prefect/deploy.py --all
```

### Bước 2: Khởi động server (Terminal 1)

```bash
prefect server start
# Truy cập UI: http://localhost:4200
```

### Bước 3: Khởi động agent (Terminal 2)

```bash
prefect agent start -p default-agent-pool
```

### Bước 4: Chạy thử

```bash
# Chạy pipeline đầy đủ
bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourly

# Chạy backfill dữ liệu lịch sử
bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
  --start-date 2024-01-01 \\
  --end-date 2024-12-31 \\
  --chunk-mode monthly
```

## ⚠️ LƯU Ý QUAN TRỌNG

### KHÔNG BAO GIỜ chạy trực tiếp bằng python

```bash
# ❌ SAI - Tạo SparkSession local
python Prefect/bronze_flow.py

# ✅ ĐÚNG - Submit lên YARN
bash scripts/spark_submit.sh Prefect/bronze_flow.py -- --mode upsert
```

## 📁 Cấu Trúc Files

```
Prefect/
├── bronze_flow.py          # Ingestion từ API
├── silver_flow.py          # Transform Bronze → Silver
├── gold_flow.py            # Load dimensions + facts
├── full_pipeline_flow.py   # Pipeline đầy đủ
├── backfill_flow.py        # Backfill lịch sử
├── deploy.py               # Script deploy
└── spark_context.py        # Quản lý SparkSession
```

## 🔄 Các Flow Chính

### 1. Hourly Pipeline (Tự động mỗi giờ)

Được schedule chạy mỗi giờ, thực hiện:
- Bronze: Cập nhật dữ liệu mới nhất
- Silver: Merge dữ liệu mới
- Gold: Cập nhật tất cả dimensions và facts

### 2. Full Pipeline (Chạy thủ công)

```bash
bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- \\
  --bronze-mode upsert \\
  --silver-mode incremental \\
  --start-date 2024-10-01 \\
  --end-date 2024-10-31
```

### 3. Backfill (Xử lý dữ liệu lịch sử)

```bash
bash scripts/spark_submit.sh Prefect/backfill_flow.py -- \\
  --start-date 2024-01-01 \\
  --end-date 2024-12-31 \\
  --chunk-mode monthly
```

## 📊 Monitoring

### Prefect UI
- **URL:** http://localhost:4200
- **Xem:** Flow runs, task status, logs, metrics

### YARN ResourceManager
- **URL:** http://khoa-master:8088
- **Xem:** Spark applications, resource usage

### Spark History Server
- **URL:** http://khoa-master:18080
- **Xem:** Job performance, stage timings

## 🐛 Xử Lý Lỗi Thường Gặp

### Lỗi: SparkSession không chạy trên YARN

**Triệu chứng:**
```
Master: local[*]
RuntimeError: Expected YARN master but got 'local[*]'
```

**Giải pháp:**
Luôn sử dụng `bash scripts/spark_submit.sh`, không chạy trực tiếp `python`

### Lỗi: Deployment không tìm thấy

**Giải pháp:**
```bash
python Prefect/deploy.py --flow hourly --update
prefect deployment ls
```

### Lỗi: Agent không chạy

**Giải pháp:**
```bash
prefect agent start -p default-agent-pool
```

## 📚 Tài Liệu Chi Tiết

- **[Prefect/README.md](README.md)** - Hướng dẫn nhanh
- **[PREFECT_FLOWS_README.md](../PREFECT_FLOWS_README.md)** - Hướng dẫn sử dụng đầy đủ
- **[docs/PREFECT_DEPLOYMENT.md](../docs/PREFECT_DEPLOYMENT.md)** - Hướng dẫn deployment chi tiết
- **[IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)** - Tóm tắt implementation

## 🎓 Các Lệnh Thường Dùng

### Chạy từng layer riêng lẻ

```bash
# Bronze only
bash scripts/spark_submit.sh Prefect/bronze_flow.py -- --mode upsert

# Silver only
bash scripts/spark_submit.sh Prefect/silver_flow.py -- --mode incremental

# Gold only
bash scripts/spark_submit.sh Prefect/gold_flow.py -- --mode all
```

### Chạy qua Prefect CLI

```bash
# Chạy hourly pipeline
prefect deployment run 'hourly-pipeline-flow/aqi-pipeline-hourly'

# Chạy backfill với parameters
prefect deployment run 'backfill-flow/aqi-pipeline-backfill' \\
  --param start_date=2024-01-01 \\
  --param end_date=2024-12-31 \\
  --param chunk_mode=monthly
```

### Kiểm tra status

```bash
# Xem deployments
prefect deployment ls

# Xem flow runs gần đây
prefect flow-run ls --limit 10

# Xem flow runs failed
prefect flow-run ls --state-type FAILED
```

## ✅ Checklist Kiểm Tra

Trước khi chạy production:

- [ ] Đã deploy tất cả flows: `python Prefect/deploy.py --all`
- [ ] Prefect server đang chạy: http://localhost:4200
- [ ] Prefect agent đang chạy: `prefect agent start`
- [ ] Đã test Bronze flow trên YARN
- [ ] Đã test Silver flow trên YARN  
- [ ] Đã test Gold flow trên YARN
- [ ] Đã test Full pipeline flow
- [ ] Đã test Backfill flow với date range nhỏ
- [ ] Đã xác nhận `master == yarn` trong logs

## 🔧 Cấu Hình

### File .env

```bash
WAREHOUSE_URI=hdfs://khoa-master:9000/warehouse/iceberg
SPARK_MASTER=yarn
ENABLE_YARN_DEFAULTS=true
SPARK_DYN_MIN=1
SPARK_DYN_MAX=50
```

### Lịch Chạy

- **Hourly pipeline:** Mỗi giờ lúc phút 0 (`0 * * * *`)
- **Timezone:** Asia/Ho_Chi_Minh (UTC+7)
- **Tự động:** Có (khi agent đang chạy)

## 🎯 Kết Luận

Tất cả yêu cầu đã được hoàn thành:

✅ Chạy trên YARN qua spark_submit.sh  
✅ Một SparkSession cho mỗi flow  
✅ Bronze flow với Prefect  
✅ Silver & Gold flows đã refactor  
✅ Full pipeline flow  
✅ Backfill flow với chunking  
✅ Deployment script với lịch hourly  
✅ Tài liệu đầy đủ  

**Sẵn sàng cho production!**

---

Nếu có thắc mắc, xem tài liệu chi tiết hoặc liên hệ team.
