# Silver Layer Modeling Guide

This note documents the structure and assumptions behind the three Silver-layer Iceberg tables that power downstream air-quality analytics. All timestamps and date fields are expressed in UTC unless explicitly stated otherwise.

## Common conventions

- **Partitioning** – Every table is stored in Iceberg with the partition spec `(location_id, days(ts_utc))` so that hourly reads prune both geography and date.
- **Units** – Measurements preserve the native units delivered by the Open-Meteo API: particulate matter in `ug/m3`, ozone/nitrogen/sulphur dioxide in `ug/m3`, carbon monoxide in `mg/m3`, and UV indices as dimensionless scores. No unit conversion occurs in the clean step.
- **Lineage metadata** – `run_id`, `ingested_at`/`computed_at`, and source run identifiers are recorded on every record for reproducibility.
- **Quality flags** – Boolean maps (`valid_flags`, `component_valid_flags`) highlight derived checks instead of dropping suspect rows so downstream logic can choose how strict to be.

## Table: aq.silver.air_quality_hourly_clean

- **Purpose** – Canonical hourly measurements with renamed pollutant columns, normalised timestamps, and preserved source metadata.
- **Inputs** – `hadoop_catalog.aq.bronze.raw_open_meteo_hourly` Bronze table.
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

Additional operational guidance (post optimization)

- Prefer running the Silver pipeline with the repository wrappers so pre-staged libraries on HDFS are used and submission overhead is minimized. Use `scripts/run_silver_range.sh` to run the full Silver pipeline for a date range (it executes `clean_hourly` then `components_hourly` then `index_hourly` in the correct order unless flags restrict steps).

- For single-step runs or ad-hoc testing, use `scripts/submit_yarn.sh` which consumes `.env` to decide whether to use the experimental `SPARK_YARN_ARCHIVE` or the recommended `SPARK_YARN_JARS`/`SPARK_PYFILES` approach. Example:

```bash
# Full range run (preferred for large windows)
bash scripts/run_silver_range.sh --start 2024-01-01T00:00:00 --end 2024-01-31T23:00:00

# Run only clean step for a small window (ad-hoc)
bash scripts/submit_yarn.sh silver/clean_hourly.py --start 2024-08-01T00:00:00 --end 2024-08-31T23:00:00 --mode replace
```

- Environment/config notes:
  - Ensure `.env` contains `SPARK_YARN_JARS` and `SPARK_PYFILES` pointing at HDFS (e.g. `hdfs://khoa-master:9000/spark/jars/*` and `hdfs://khoa-master:9000/spark/python/pyspark.zip,...`). This allows the submit wrapper to avoid uploading hundreds of jars on every job.
  - `SPARK_YARN_ARCHIVE` is supported but experimental; only set it if you purposely created a validated Spark distribution tarball on HDFS.

- Run ordering and idempotency:
  - The wrapper scripts support `--mode replace` to remove existing Silver records for the window before writing. This is useful for deterministic reruns.
  - When running the full range, the range script handles look-back for components (so you don't need to manage historical padding manually).

- Validation and quick checks:
  - After a run, open `notebooks/silver_validation.ipynb` or run the lightweight validation SQL in the notebook to verify row counts per `location_id` and date ranges.
  - Monitor Spark submission logs; if you see many "Not copying" messages for `/spark/jars/` then the pre-staged approach worked correctly.

## Troubleshooting Common Issues

### OutOfMemoryError During Large Date Ranges

**Symptoms**: Executor failures with `java.lang.OutOfMemoryError: Java heap space` during Iceberg writes, especially when processing ranges longer than 6 months.

**Root Cause**: Default memory settings (512MB executor memory + 256MB overhead) are insufficient for processing large amounts of data in a single Spark application.

**Solutions**:

1. **Automatic Chunking** (Recommended): Use the built-in chunking feature in `run_silver_range.sh`:
   ```bash
   # Automatically splits ranges >6 months into chunks
   bash scripts/run_silver_range.sh --start 2024-01-01 --end 2025-09-30
   
   # Custom chunk size (4 months)
   bash scripts/run_silver_range.sh --start 2024-01-01 --end 2025-09-30 --chunk-months 4
   
   # Disable chunking for testing (not recommended for large ranges)
   bash scripts/run_silver_range.sh --start 2024-01-01 --end 2025-09-30 --no-chunking
   ```

2. **Memory Tuning**: Adjust memory settings in `.env` for your cluster capacity:
   ```properties
   # For large ranges - requires YARN_MAX_ALLOCATION_MB >= 2048
   SPARK_EXECUTOR_INSTANCES=2
   SPARK_EXECUTOR_CORES=2
   SPARK_EXECUTOR_MEMORY=1536m
   SPARK_EXECUTOR_MEMORY_OVERHEAD=512m
   SPARK_DRIVER_MEMORY=1g
   YARN_MAX_ALLOCATION_MB=4096
   ```

3. **Incremental Processing**: Process smaller date ranges sequentially:
   ```bash
   # Process monthly chunks manually
   for month in 01 02 03 04 05 06 07 08 09 10 11 12; do
     bash scripts/run_silver_range.sh --start "2024-${month}-01T00:00:00" --end "2024-${month}-31T23:00:00"
   done
   ```

### YARN Application Failures

**Symptoms**: `YARN application has exited unexpectedly with state FAILED! Max number of executor failures (3) reached`

**Common Causes**:
- Insufficient memory allocation for executors
- Node hardware issues or resource contention
- Network connectivity problems between YARN nodes

**Solutions**:
1. Check YARN ResourceManager logs for detailed failure reasons
2. Increase memory overhead: `SPARK_EXECUTOR_MEMORY_OVERHEAD=768m` 
3. Reduce parallelism: `SPARK_EXECUTOR_INSTANCES=1` for problematic ranges
4. Use chunked processing to reduce memory pressure per job

### Performance Optimization Tips

1. **Monitor Memory Usage**: Check Spark UI for GC time and memory utilization
2. **Adjust Shuffle Partitions**: Increase `SPARK_SQL_SHUFFLE_PARTITIONS` for large datasets
3. **Enable Kryo Serialization**: Already configured in submit script for better performance
4. **Use Pre-staged Jars**: Verify `SPARK_YARN_JARS` is configured to avoid jar upload overhead
