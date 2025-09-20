-- =========================
-- AQ Lakehouse: One-shot reset & prepare
-- Yêu cầu: Spark 4.x + Iceberg (Hadoop catalog tên hadoop_catalog)
-- =========================

-- (Tuỳ chọn) In thông báo nhỏ để dễ đọc log
SELECT 'Reset AQ lakehouse (Bronze/Silver/Gold) starting...' AS info;

-- 0) Tạo namespace nếu chưa có (cả catalog Iceberg và catalog mặc định cho VIEW)
CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.aq;
CREATE NAMESPACE IF NOT EXISTS spark_catalog.aq;

-- 1) BRONZE: đảm bảo tồn tại bảng & TRUNCATE
CREATE TABLE IF NOT EXISTS hadoop_catalog.aq.raw_open_meteo_hourly (
  location_id            STRING,
  latitude               DOUBLE,
  longitude              DOUBLE,
  ts                     TIMESTAMP,
  aerosol_optical_depth  DOUBLE,
  pm2_5                  DOUBLE,
  pm10                   DOUBLE,
  dust                   DOUBLE,
  nitrogen_dioxide       DOUBLE,
  ozone                  DOUBLE,
  sulphur_dioxide        DOUBLE,
  carbon_monoxide        DOUBLE,
  uv_index               DOUBLE,
  uv_index_clear_sky     DOUBLE,
  source                 STRING,
  batch_id               STRING,
  ingested_at            TIMESTAMP
)
USING iceberg
PARTITIONED BY (location_id, month(ts))   -- spec gợi ý; nếu bảng đã tồn tại thì dòng này không đổi spec cũ
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

TRUNCATE TABLE hadoop_catalog.aq.raw_open_meteo_hourly;

-- Housekeeping Iceberg cho Bronze (an toàn ngay cả khi bảng đang trống)
CALL hadoop_catalog.system.remove_orphan_files('aq.raw_open_meteo_hourly');
CALL hadoop_catalog.system.expire_snapshots('aq.raw_open_meteo_hourly', CURRENT_TIMESTAMP - INTERVAL 1 HOURS);

-- 2) DIM (tuỳ chọn) – dọn sạch để rerun
CREATE TABLE IF NOT EXISTS hadoop_catalog.aq.dim_locations (
  location_key   INT,
  location_id    STRING,
  latitude       DOUBLE,
  longitude      DOUBLE,
  city           STRING,
  country        STRING,
  is_active      BOOLEAN,
  effective_from DATE,
  effective_to   DATE
)
USING iceberg
TBLPROPERTIES ('format-version'='2');
TRUNCATE TABLE hadoop_catalog.aq.dim_locations;

-- 3) GOLD: đảm bảo tồn tại bảng & TRUNCATE
CREATE TABLE IF NOT EXISTS hadoop_catalog.aq.gold_air_quality_daily (
  city         STRING,
  date         DATE,
  aqi_us_mean  DOUBLE,
  pm25_mean    DOUBLE,
  pm10_mean    DOUBLE,
  no2_mean     DOUBLE,
  o3_mean      DOUBLE,
  so2_mean     DOUBLE,
  co_mean      DOUBLE,
  uv_mean      DOUBLE
)
USING iceberg
PARTITIONED BY (city, month(date))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet');

TRUNCATE TABLE hadoop_catalog.aq.gold_air_quality_daily;

-- Housekeeping Iceberg cho Gold
CALL hadoop_catalog.system.remove_orphan_files('aq.gold_air_quality_daily');
CALL hadoop_catalog.system.expire_snapshots('aq.gold_air_quality_daily', CURRENT_TIMESTAMP - INTERVAL 1 HOURS);

-- 4) SILVER VIEW: tạo/replace để luôn trỏ đọc trực tiếp Bronze
CREATE OR REPLACE VIEW spark_catalog.aq.v_silver_air_quality_hourly AS
SELECT
  location_id,
  ts,
  pm2_5,
  pm10,
  nitrogen_dioxide AS no2,
  ozone            AS o3,
  sulphur_dioxide  AS so2,
  carbon_monoxide  AS co,
  uv_index,
  uv_index_clear_sky,
  aerosol_optical_depth,
  dust,
  latitude,
  longitude,
  source,
  batch_id,
  ingested_at
FROM hadoop_catalog.aq.raw_open_meteo_hourly;

-- (tuỳ chọn) daily view để BI test nhanh
CREATE OR REPLACE VIEW spark_catalog.aq.v_silver_air_quality_daily AS
SELECT
  location_id,
  DATE(ts)                                  AS date,
  AVG(pm2_5)                AS pm25_mean,
  AVG(pm10)                 AS pm10_mean,
  AVG(nitrogen_dioxide)     AS no2_mean,
  AVG(ozone)                AS o3_mean,
  AVG(sulphur_dioxide)      AS so2_mean,
  AVG(carbon_monoxide)      AS co_mean,
  AVG(uv_index)             AS uv_mean
FROM hadoop_catalog.aq.raw_open_meteo_hourly
GROUP BY location_id, DATE(ts);

-- 5) Sanity prints (giúp biết script đã chạy xong)
SELECT 'Reset done. Re-run your ingest job; Silver views tự cập nhật từ Bronze.' AS info;
