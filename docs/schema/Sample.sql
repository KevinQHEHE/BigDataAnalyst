CREATE TABLE `dim_date` (
  `date_key` INT PRIMARY KEY NOT NULL,
  `date_value` DATE NOT NULL,
  `day_of_month` TINYINT,
  `day_of_week` TINYINT,
  `month` TINYINT,
  `month_name` VARCHAR(16),
  `quarter` TINYINT,
  `year` SMALLINT,
  `is_weekend` BOOLEAN
);

CREATE TABLE `dim_time` (
  `time_key` INT PRIMARY KEY NOT NULL,
  `time_value` TIME NOT NULL,
  `hour` TINYINT,
  `work_shift` VARCHAR(32)
);

CREATE TABLE `dim_location` (
  `location_key` VARCHAR(64) PRIMARY KEY NOT NULL,
  `location_name` VARCHAR(128) NOT NULL,
  `latitude` DOUBLE,
  `longitude` DOUBLE,
  `timezone` VARCHAR(64)
);

CREATE TABLE `dim_pollutant` (
  `pollutant_code` VARCHAR(32) PRIMARY KEY NOT NULL,
  `display_name` VARCHAR(64),
  `unit_default` VARCHAR(16),
  `aqi_timespan` VARCHAR(8)
);

CREATE TABLE `fact_air_quality_hourly` (
  `record_id` CHAR(36) PRIMARY KEY NOT NULL,
  `location_key` VARCHAR(64) NOT NULL,
  `date_key` INT NOT NULL,
  `time_key` INT NOT NULL,
  `ts_utc` DATETIME NOT NULL,
  `date_utc` DATE NOT NULL,
  `source_system` VARCHAR(32) DEFAULT 'open-meteo',
  `model_domain` VARCHAR(32),
  `request_timezone` VARCHAR(64),
  `aqi` INT,
  `aqi_pm25` INT,
  `aqi_pm10` INT,
  `aqi_no2` INT,
  `aqi_o3` INT,
  `aqi_so2` INT,
  `aqi_co` INT,
  `aqi_category` VARCHAR(32),
  `dominant_pollutant` VARCHAR(32),
  `pm25` DOUBLE,
  `pm10` DOUBLE,
  `o3` DOUBLE,
  `no2` DOUBLE,
  `so2` DOUBLE,
  `co` DOUBLE,
  `aod` DOUBLE,
  `dust` DOUBLE,
  `uv_index` DOUBLE,
  `carbon_dioxide` DOUBLE,
  `data_completeness` DOUBLE
);

CREATE TABLE `fact_city_daily` (
  `daily_record_id` CHAR(36) PRIMARY KEY NOT NULL,
  `location_key` VARCHAR(64) NOT NULL,
  `date_key` INT NOT NULL,
  `date_utc` DATE NOT NULL,
  `aqi_daily_max` INT,
  `dominant_pollutant_daily` VARCHAR(32),
  `hours_in_cat_good` INT,
  `hours_in_cat_moderate` INT,
  `hours_in_cat_usg` INT,
  `hours_in_cat_unhealthy` INT,
  `hours_in_cat_very_unhealthy` INT,
  `hours_in_cat_hazardous` INT,
  `hours_measured` INT,
  `data_completeness` DOUBLE
);

CREATE TABLE `fact_episode` (
  `episode_id` CHAR(36) PRIMARY KEY NOT NULL,
  `location_key` VARCHAR(64) NOT NULL,
  `start_ts_utc` DATETIME NOT NULL,
  `end_ts_utc` DATETIME NOT NULL,
  `start_date_utc` DATE NOT NULL,
  `duration_hours` INT,
  `peak_aqi` INT,
  `hours_flagged` INT,
  `dominant_pollutant` VARCHAR(32),
  `rule_code` VARCHAR(64)
);

CREATE UNIQUE INDEX `uk_dim_date_value` ON `dim_date` (`date_value`);

CREATE UNIQUE INDEX `uk_dim_time_value` ON `dim_time` (`time_value`);

CREATE UNIQUE INDEX `uk_dim_location_name` ON `dim_location` (`location_name`);

CREATE INDEX `ix_faqh_loc_date` ON `fact_air_quality_hourly` (`location_key`, `date_utc`);

CREATE INDEX `ix_faqh_ts` ON `fact_air_quality_hourly` (`ts_utc`);

CREATE INDEX `ix_faqh_dim_keys` ON `fact_air_quality_hourly` (`date_key`, `time_key`);

CREATE UNIQUE INDEX `uk_fcd_loc_date` ON `fact_city_daily` (`location_key`, `date_utc`);

CREATE INDEX `ix_fcd_loc` ON `fact_city_daily` (`location_key`);

CREATE INDEX `ix_fcd_date` ON `fact_city_daily` (`date_utc`);

CREATE INDEX `ix_ep_loc_start` ON `fact_episode` (`location_key`, `start_ts_utc`);

CREATE INDEX `ix_ep_startdate` ON `fact_episode` (`start_date_utc`);

ALTER TABLE `fact_air_quality_hourly` ADD CONSTRAINT `fk_faqh_location` FOREIGN KEY (`location_key`) REFERENCES `dim_location` (`location_key`) ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE `fact_air_quality_hourly` ADD CONSTRAINT `fk_faqh_date` FOREIGN KEY (`date_key`) REFERENCES `dim_date` (`date_key`) ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE `fact_air_quality_hourly` ADD CONSTRAINT `fk_faqh_time` FOREIGN KEY (`time_key`) REFERENCES `dim_time` (`time_key`) ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE `fact_air_quality_hourly` ADD CONSTRAINT `fk_faqh_dom_pol` FOREIGN KEY (`dominant_pollutant`) REFERENCES `dim_pollutant` (`pollutant_code`) ON DELETE SET NULL ON UPDATE CASCADE;

ALTER TABLE `fact_city_daily` ADD CONSTRAINT `fk_fcd_location` FOREIGN KEY (`location_key`) REFERENCES `dim_location` (`location_key`) ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE `fact_city_daily` ADD CONSTRAINT `fk_fcd_date` FOREIGN KEY (`date_key`) REFERENCES `dim_date` (`date_key`) ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE `fact_city_daily` ADD CONSTRAINT `fk_fcd_dom_pol` FOREIGN KEY (`dominant_pollutant_daily`) REFERENCES `dim_pollutant` (`pollutant_code`) ON DELETE SET NULL ON UPDATE CASCADE;

ALTER TABLE `fact_episode` ADD CONSTRAINT `fk_ep_location` FOREIGN KEY (`location_key`) REFERENCES `dim_location` (`location_key`) ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE `fact_episode` ADD CONSTRAINT `fk_ep_dom_pol` FOREIGN KEY (`dominant_pollutant`) REFERENCES `dim_pollutant` (`pollutant_code`) ON DELETE SET NULL ON UPDATE CASCADE;
