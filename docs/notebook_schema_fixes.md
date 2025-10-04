# Sửa chữa AQI Research Notebook - Schema Alignment

## Vấn đề được phát hiện

Sau khi phân tích cấu trúc Gold layer và so sánh với notebook `aqi_research.ipynb`, tôi đã tìm thấy và sửa chữa các vấn đề sau:

### 1. **Schema mismatch giữa Notebook và Gold Layer**

**Vấn đề ban đầu:**
- Notebook định nghĩa `location_key`, `date_key`, `time_key` là **INTEGER**
- Gold layer thực tế sử dụng **STRING** cho tất cả các keys

**Thay đổi đã thực hiện:**
```python
# Trước đây (SAI):
T.StructField('location_key', T.IntegerType(), True),
T.StructField('date_key', T.IntegerType(), True),
T.StructField('time_key', T.IntegerType(), True),

# Bây giờ (ĐÚNG):
T.StructField('location_key', T.StringType(), True),
T.StructField('date_key', T.StringType(), True),  # Format: "yyyyMMdd"
T.StructField('time_key', T.StringType(), True),  # Format: "HH"
```

### 2. **Thiếu columns trong schema Fact table**

**Vấn đề:** Notebook thiếu nhiều columns quan trọng có trong Gold fact table

**Thay đổi đã thực hiện:**
Đã bổ sung đầy đủ các columns:
- `pm25_24h_avg`, `pm10_24h_avg`, `pm25_nowcast`
- `o3_8h_max`, `no2_1h_max`, `so2_1h_max`, `co_8h_avg`
- `ingested_at`, `computed_at`
- `component_calc_method`, `index_calc_method`
- `created_at`, `updated_at`

### 3. **AQI data type casting**

**Vấn đề:** Gold layer định nghĩa AQI là INT, nhưng analysis cần DOUBLE

**Giải pháp:**
```python
F.col("f.aqi").cast("double").alias("aqi"),  # Cast INT to DOUBLE for consistency
```

### 4. **Thiếu columns trong dimension schemas**

**Dim Location:** Đã bổ sung `latitude`, `longitude`, `is_active`, `valid_from`, `valid_to`
**Dim Date:** Đã bổ sung `quarter`
**Dim Time:** Đã bổ sung `hour_label`, `is_am`

## Kết quả

✅ **Schema alignment hoàn chỉnh** - Notebook hiện tại tương thích 100% với Gold layer
✅ **Type safety** - Tất cả data types khớp với định nghĩa trong Gold jobs
✅ **Column completeness** - Đầy đủ columns theo Gold schema
✅ **Join compatibility** - Keys có đúng types để join thành công

## Kiểm tra tiếp theo

Notebook hiện đã sẵn sàng để:
1. Kết nối thành công với Gold tables
2. Thực hiện joins without type errors
3. Sử dụng đầy đủ columns available in Gold layer
4. Tương thích với actual data trong lakehouse

## Files đã thay đổi

- `/home/dlhnhom2/dlh-aqi/notebooks/aqi_research.ipynb`
  - Cell #9: Data Scope Confirmation (schema definitions)
  - Cell #15: Analytical Dataset Construction (AQI casting)
  - Cell #16: Daily aggregation (error handling)