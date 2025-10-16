# Tối ưu Silver/Gold Flow cho Incremental Processing

## Vấn đề hiện tại

**Bronze:** ✅ Incremental (chỉ ingest data mới từ API)
**Silver:** ❌ Full scan Bronze table mỗi lần chạy
**Gold:** ❌ Full scan Silver table mỗi lần chạy

→ **Lãng phí tài nguyên nghiêm trọng!**

## Giải pháp

### Option 1: Auto-detect new data (Recommended)

Thêm logic vào Silver/Gold flows để tự động detect data mới:

```python
# Silver flow - tự động tìm data mới từ Bronze
def get_new_bronze_data_range(spark, bronze_table, silver_table):
    """Get date range of new data in Bronze that's not in Silver yet"""
    
    # Get max timestamp in Silver
    silver_max = spark.sql(f"""
        SELECT MAX(ts_utc) as max_ts 
        FROM {silver_table}
    """).collect()[0]['max_ts']
    
    if silver_max is None:
        # Silver empty, process all Bronze
        return None, None
    
    # Get min/max timestamps in Bronze > silver_max
    bronze_new = spark.sql(f"""
        SELECT 
            MIN(date_utc) as min_date,
            MAX(date_utc) as max_date
        FROM {bronze_table}
        WHERE ts_utc > '{silver_max}'
    """).collect()[0]
    
    if bronze_new['min_date'] is None:
        # No new data
        return "NO_NEW_DATA", "NO_NEW_DATA"
    
    return bronze_new['min_date'], bronze_new['max_date']

# Usage in silver_transformation_flow:
if mode == "incremental" and not start_date:
    # Auto-detect new data
    start_date, end_date = get_new_bronze_data_range(spark, bronze_table, silver_table)
    
    if start_date == "NO_NEW_DATA":
        print("✓ Silver already up-to-date, skipping")
        return {"status": "skipped", "reason": "no_new_data"}
    
    print(f"Auto-detected new data: {start_date} to {end_date}")
```

### Option 2: Pass date range từ Bronze → Silver → Gold

Sửa `hourly_pipeline_flow` để truyền date range:

```python
@flow
def hourly_pipeline_flow(...):
    # 1. Bronze ingest
    bronze_result = bronze_ingestion_flow(mode="upsert", ...)
    
    # Extract date range from Bronze result
    bronze_dates = bronze_result.get('date_ranges', [])
    if bronze_dates:
        min_date = min(d['start'] for d in bronze_dates)
        max_date = max(d['end'] for d in bronze_dates)
        
        # 2. Silver: chỉ transform data mới
        silver_result = silver_transformation_flow(
            mode="incremental",
            start_date=min_date,  # ← Truyền date range
            end_date=max_date,
            ...
        )
        
        # 3. Gold: chỉ update data mới
        gold_result = gold_pipeline_flow(
            mode="incremental",
            start_date=min_date,  # ← Truyền date range
            end_date=max_date,
            ...
        )
```

### Option 3: Iceberg Incremental Read (Advanced)

Sử dụng Iceberg snapshot để đọc chỉ data thay đổi:

```python
# Read only changes since last snapshot
df_new = spark.read \
    .format("iceberg") \
    .option("start-snapshot-id", last_processed_snapshot) \
    .option("end-snapshot-id", current_snapshot) \
    .table(bronze_table)
```

## So sánh

| Option | Pros | Cons | Complexity |
|--------|------|------|------------|
| **1: Auto-detect** | Simple, automatic | Requires query per flow | Low |
| **2: Pass range** | Explicit, clear | Must modify Bronze to return range | Medium |
| **3: Snapshot** | Most efficient | Requires tracking snapshots | High |

## Khuyến nghị

**Dùng Option 1** (Auto-detect) vì:
- ✅ Simple implementation
- ✅ Không cần thay đổi Bronze logic
- ✅ Tự động phát hiện data mới
- ✅ Dễ maintain

**Implementation steps:**

1. Thêm helper function `get_new_data_range()` vào `silver/transform_bronze_to_silver.py`
2. Sửa `silver_transformation_flow()` để auto-detect khi `mode="incremental"` và không có `start_date`
3. Tương tự cho Gold flows

## Ước tính tiết kiệm

**Hiện tại:** 
- Bronze: 500k records
- Silver đọc: 500k records mỗi lần
- Thời gian: ~2-3 phút

**Sau optimization:**
- Bronze ingest: +100 records (1 giờ mới)
- Silver đọc: chỉ 100 records
- Thời gian: ~5-10 giây
- **→ Tiết kiệm 95% tài nguyên!** 🚀

## Code sample

Xem file: `docs/incremental_optimization.md` để có code đầy đủ.
