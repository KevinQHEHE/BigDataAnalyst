# Silver Layer Modeling Guide

This note documents the structure and assumptions behind the three Silver-layer Iceberg tables that power downstream air-quality analytics. All timestamps and date fields are expressed in UTC unless explicitly stated otherwise.

## Common conventions

- **Partitioning** – Every table is stored in Iceberg with the partition spec `(location_id, days(ts_utc))` so that hourly reads prune both geography and date.
- **Units** – Measurements preserve the native units delivered by the Open-Meteo API: particulate matter in `ug/m3`, ozone/nitrogen/sulphur dioxide in `ug/m3`, carbon monoxide in `mg/m3`, and UV indices as dimensionless scores. No unit conversion occurs in the clean step.
- **Lineage metadata** – `run_id`, `ingested_at`/`computed_at`, and source run identifiers are recorded on every record for reproducibility.
- **Quality flags** – Boolean maps (`valid_flags`, `component_valid_flags`) highlight derived checks instead of dropping suspect rows so downstream logic can choose how strict to be.

## Table: aq.silver.air_quality_hourly_clean

- **Purpose** – Canonical hourly measurements with renamed pollutant columns, normalised timestamps, and preserved source metadata.
- **Inputs** – `hadoop_catalog.aq.raw_open_meteo_hourly` Bronze table.
- **Key columns**
  - `location_id`, `ts_utc`, `date_utc` – spatial/temporal keys in UTC.
  - Pollutants: `aod`, `pm25`, `pm10`, `dust`, `no2`, `o3`, `so2`, `co`, `uv_index`, `uv_index_clear_sky`.
  - Metadata: `source`, `bronze_run_id`, `bronze_ingested_at`, `run_id`, `ingested_at`, optional `notes` string passed from job parameters.
  - `valid_flags` – Map with flags such as `pm25_nonneg`, `o3_nonneg`, `ts_present`, etc. Each flag is `true` when the corresponding measurement is non-null and non-negative (for numeric values) or present (for coordinates / timestamp).
- **Assumptions**
  - Bronze timestamps are already UTC; they are re-cast via `to_utc_timestamp` for safety.
  - Negative readings from Bronze were already coerced to null; flags ensure any residual negative values are caught.
  - Latitude/longitude are passed through unchanged to support spatial joins in later layers.

## Table: aq.silver.aq_components_hourly

- **Purpose** – Store intermediate rolling statistics needed to build AQI or other exposure scores.
- **Inputs** – Silver clean table.
- **Derived metrics**
  - `pm25_24h_avg`, `pm10_24h_avg` – 24 hour trailing averages. Require at least 18 non-null hours (`PM_MIN_VALID_HOURS`).
  - `o3_8h_max`, `co_8h_max` – 8 hour trailing maxima. Require at least 6 valid hours.
  - `no2_1h_max`, `so2_1h_max` – Most recent hourly values (no windowing beyond continuity check).
- **Continuity rules**
  - Windows are partitioned by `location_id` and respect chronological order. The job loads look-back rows so the first hour in the requested window has enough history.
  - `component_valid_flags` captures sufficiency flags (`pm25_24h_sufficient`, `o3_8h_sufficient`, etc.), a `continuous_hour` flag that verifies 1-hour spacing, and presence flags for NO2/SO2.
- **Metadata** – `calc_method` defaults to `simple_rolling_v1`; adjust when experimenting with NowCast or alternative rules.

## Table: aq.silver.aq_index_hourly

- **Purpose** – Provide final hourly AQI-style scores plus pollutant-specific sub-indices for reporting.
- **Inputs** – Components table.
- **Calculations**
  - Converts component concentrations to EPA-style breakpoints. Gaseous pollutants use conversions based on molar mass and a molar volume of 24.45 L at standard conditions.
  - `linear_aqi` interpolates within the official breakpoint tables; values above the top breakpoint extend the final slope.
  - Per-pollutant AQIs: `aqi_pm25`, `aqi_pm10`, `aqi_o3`, `aqi_no2`, `aqi_so2`, `aqi_co`.
  - Overall `aqi` is the greatest pollutant AQI. `category` assigns the EPA label (`Good`, `Moderate`, … `Hazardous`).
  - `dominant_pollutant` picks the pollutant contributing the max AQI (ties resolved by `array_max`, effectively any highest AQI value).
- **Metadata** – `calc_method` default `epa_like_v1`, `run_id`, `computed_at`, `date_utc` provided for lineage and partitioning.

## Quality-flag reference

| Flag | Table | Meaning |
|------|-------|---------|
| `pm25_nonneg`, `o3_nonneg`, … | Clean | Source measurement is present and non-negative. |
| `ts_present`, `latitude_present`, `longitude_present` | Clean | Location/timestamp metadata available. |
| `pm25_24h_sufficient`, `pm10_24h_sufficient` | Components | Rolling window has >= 18 valid hours. |
| `o3_8h_sufficient`, `co_8h_sufficient` | Components | Rolling window has >= 6 valid hours. |
| `no2_present`, `so2_present` | Components | Latest hourly value is non-null. |
| `continuous_hour` | Components | Current record follows an exact 1-hour cadence from previous hour for the location. |

## Operational notes

- Run windows in Silver jobs should align: execute `clean_hourly`, then `components_hourly`, then `index_hourly` for the same `--start/--end` to guarantee downstream completeness.
- When gaps exist or a rerun is necessary, use `--mode replace` to delete existing rows for the requested window before merging new results.
- Use `notebooks/silver_validation.ipynb` for day-level row counts, null-ratio inspection, and SQL sanity-checks after each batch run.
