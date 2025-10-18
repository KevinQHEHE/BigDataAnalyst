-- =========================================================
-- NAMESPACES
-- =========================================================
CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.lh.bronze;
CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.lh.silver;
CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.lh.gold;

-- =========================================================
-- BRONZE
-- Raw landing from Open-Meteo (no data transformation)
-- Natural upsert key: (location_key, ts_utc)
-- Partition: (date_utc, location_key)
-- =========================================================
CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.bronze.open_meteo_hourly (
  -- natural keys
  location_key        STRING       NOT NULL,      -- 'hanoi','hcmc',...
  ts_utc              TIMESTAMP    NOT NULL,      -- UTC time
  -- partition
  date_utc            DATE         NOT NULL,      -- derived from ts_utc
  -- location metadata (for reconciliation if needed)
  latitude            DOUBLE,
  longitude           DOUBLE,
  -- AQI (pre-calculated using US standard)
  aqi                 INT,
  aqi_pm25            INT,
  aqi_pm10            INT,
  aqi_no2             INT,
  aqi_o3              INT,
  aqi_so2             INT,
  aqi_co              INT,
  -- hourly concentrations (mostly µg/m³; uv_index is index; CO2 in ppm)
  pm25                DOUBLE,
  pm10                DOUBLE,
  o3                  DOUBLE,
  no2                 DOUBLE,
  so2                 DOUBLE,
  co                  DOUBLE,
  aod                 DOUBLE,
  dust                DOUBLE,
  uv_index            DOUBLE,
  co2                 DOUBLE,
  -- source metadata
  model_domain        STRING,                     -- cams_europe|cams_global|auto (if available)
  request_timezone    STRING,                     -- 'UTC'|...
  _ingested_at        TIMESTAMP
)
USING PARQUET
PARTITIONED BY (date_utc, location_key)
TBLPROPERTIES ('parquet.compression'='ZSTD');


-- =========================================================
-- SILVER
-- Standardized & enriched: add date_key/time_key, cast types
-- Upsert key: (location_key, ts_utc)
-- Partition: (date_utc, location_key)
-- =========================================================
CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.silver.air_quality_hourly_clean (
  location_key        STRING       NOT NULL,
  ts_utc              TIMESTAMP    NOT NULL,
  date_utc            DATE         NOT NULL,
  -- dimension join keys
  date_key            INT,                          -- YYYYMMDD
  time_key            INT,                          -- HH00 (0..2300)

  -- AQI & sub-indices
  aqi                 INT,
  aqi_pm25            INT,
  aqi_pm10            INT,
  aqi_no2             INT,
  aqi_o3              INT,
  aqi_so2             INT,
  aqi_co              INT,

  -- hourly concentrations
  pm25                DOUBLE,
  pm10                DOUBLE,
  o3                  DOUBLE,
  no2                 DOUBLE,
  so2                 DOUBLE,
  co                  DOUBLE,
  aod                 DOUBLE,
  dust                DOUBLE,
  uv_index            DOUBLE,
  co2                 DOUBLE,

  -- metadata
  model_domain        STRING,
  request_timezone    STRING,
  _ingested_at        TIMESTAMP
)
USING PARQUET
PARTITIONED BY (date_utc, location_key)
TBLPROPERTIES ('parquet.compression'='ZSTD');


-- =========================================================
-- GOLD (DIM)
-- Conformed dimensions in GOLD for clean, stable BI joins
-- =========================================================

CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.dim_date (
  date_key     INT,          -- YYYYMMDD
  date_value   DATE,
  day_of_month INT,
  day_of_week  INT,          -- 1=Mon..7=Sun
  week_of_year INT,
  month        INT,
  month_name   STRING,
  quarter      INT,
  year         INT,
  is_weekend   BOOLEAN
)
USING iceberg
TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.dim_time (
  time_key    INT,           -- HH00 (0..2300)
  time_value  STRING,        -- 'HH:MM'
  hour        INT,           -- 0..23
  work_shift  STRING         -- example: morning/evening/night
)
USING iceberg
TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.dim_location (
  location_key   STRING,     -- 'hanoi','hcmc',...
  location_name  STRING,
  latitude       DOUBLE,
  longitude      DOUBLE,
  timezone       STRING
)
USING iceberg
TBLPROPERTIES ('format-version'='2');

CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.dim_pollutant (
  pollutant_code STRING,     -- 'pm25','pm10','o3','no2','so2','co','aod','dust','uv_index'
  display_name   STRING,
  unit_default   STRING,     -- 'µg/m³','index','ppm',...
  aqi_timespan   STRING      -- '24h','8h','1h' or NULL
)
USING iceberg
TBLPROPERTIES ('format-version'='2');

-- =========================================================
-- GOLD (FACT)
-- =========================================================

-- Hourly fact: Upsert key (location_key, ts_utc) | Partition (date_utc, location_key)
CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.fact_air_quality_hourly (
  record_id           STRING,         -- uuid() if needed
  location_key        STRING       NOT NULL,
  ts_utc              TIMESTAMP    NOT NULL,

  date_utc            DATE         NOT NULL,
  date_key            INT,
  time_key            INT,

  aqi                 INT,
  aqi_pm25            INT,
  aqi_pm10            INT,
  aqi_no2             INT,
  aqi_o3              INT,
  aqi_so2             INT,
  aqi_co              INT,

  pm25                DOUBLE,
  pm10                DOUBLE,
  o3                  DOUBLE,
  no2                 DOUBLE,
  so2                 DOUBLE,
  co                  DOUBLE,
  aod                 DOUBLE,
  dust                DOUBLE,
  uv_index            DOUBLE,
  co2                 DOUBLE,

  dominant_pollutant  STRING,        -- argmax(aqi_*)
  data_completeness   DOUBLE         -- % of measured columns with values (0..100)
)
USING PARQUET
PARTITIONED BY (date_utc, location_key)
TBLPROPERTIES ('parquet.compression'='ZSTD');


-- Daily fact: Upsert key (location_key, date_utc) | Partition (date_utc, location_key)
CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.fact_city_daily (
  daily_record_id             STRING,
  location_key                STRING       NOT NULL,
  date_utc                    DATE         NOT NULL,
  date_key                    INT,

  aqi_daily_max               INT,
  dominant_pollutant_daily    STRING,

  hours_in_cat_good           INT,
  hours_in_cat_moderate       INT,
  hours_in_cat_usg            INT,
  hours_in_cat_unhealthy      INT,
  hours_in_cat_very_unhealthy INT,
  hours_in_cat_hazardous      INT,

  hours_measured              INT,
  data_completeness           DOUBLE
)
USING PARQUET
PARTITIONED BY (date_utc, location_key)
TBLPROPERTIES ('parquet.compression'='ZSTD');


-- Episode: Upsert key (location_key, start_ts_utc) | Partition (start_date_utc, location_key)
CREATE TABLE IF NOT EXISTS hadoop_catalog.lh.gold.fact_episode (
  episode_id          STRING,
  location_key        STRING       NOT NULL,

  start_ts_utc        TIMESTAMP    NOT NULL,
  end_ts_utc          TIMESTAMP    NOT NULL,
  start_date_utc      DATE         NOT NULL,

  duration_hours      INT,
  peak_aqi            INT,
  hours_flagged       INT,
  dominant_pollutant  STRING,
  rule_code           STRING       
)
USING PARQUET
PARTITIONED BY (start_date_utc, location_key)
TBLPROPERTIES ('parquet.compression'='ZSTD');
