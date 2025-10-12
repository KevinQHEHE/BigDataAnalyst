# Air Quality Lakehouse Readiness Guide

This repository prepares a local lakehouse stack for analysing air-quality data (Open-Meteo hourly feeds, daily KPIs, episodes, forecasting). It documents what is already installed on the host, how to verify health quickly, the tables we expect to build, and the analytics backlog.

## System Snapshot

| Layer | Installed | Notes |
| --- | --- | --- |
| OS | Ubuntu 22.04.5 LTS (kernel 6.6.87 WSL2) | `cat /etc/os-release` |
| Hadoop | Hadoop 3.4.1 (HDFS safe mode currently **ON**) | `hdfs dfsadmin -report` |
| Spark | Spark 4.0.0 + PySpark 4.0.1 | `spark-submit --version` |
| Iceberg | Runtime jar `~/spark/jars/iceberg-spark-runtime-4.0_2.13-1.10.0.jar` | Hadoop catalog rooted at `hdfs:///warehouse/iceberg_test` |
| Prefect | Prefect CLI 3.4.22 (ephemeral profile) | Emits Pydantic warning; `prefect version` shows defaults |
| Spark Thrift Server | Not listening on `localhost:10000` | Needs manual start before BI tools connect |
| Superset | CLI present (5.0.0) but fails at startup | Breaks on `sqlalchemy.orm.eagerload` removed in SQLAlchemy 2.x |
| MLflow | CLI 3.4.0 | Imports cleanly; Pydantic deprecation warning only |

### Quick Health Check

Run the aggregated probe script:

```bash
./scripts/health_check.sh
```

The script prints `[OK]`, `[WARN]`, `[ERROR]` per check and exits with status `0/1/2`. It covers:

- Operating system and kernel metadata.
- Python 3.10, PySpark, MLflow imports plus key pip packages.
- HDFS reachability, namespace contents, and Iceberg metadata for `${ICEBERG_NAMESPACE}.${ICEBERG_TABLE}` (defaults to `default.air_quality`).
- Spark binaries and Iceberg catalog enumeration via `spark-sql`.
- Prefect CLI version and configuration profile snapshot.
- Service connectivity (Spark Thrift Server port, Superset CLI, MLflow CLI).

> Current warnings (2025-10-12):
> - Spark Thrift Server port `10000` is closed. Start the service before BI workloads:
>   ```bash
>   /home/dlhnhom2/spark/sbin/start-thriftserver.sh --master local[2] \
>     --conf spark.jars=/home/dlhnhom2/spark/jars/iceberg-spark-runtime-4.0_2.13-1.10.0.jar \
>     --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
>     --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog \
>     --conf spark.sql.catalog.hadoop_catalog.type=hadoop \
>     --conf spark.sql.catalog.hadoop_catalog.warehouse=hdfs:///warehouse/iceberg_test
>   ```
> - Superset 5.0.0 fails against SQLAlchemy 2.0.43. Downgrade SQLAlchemy:
>   ```bash
>   python3 -m pip install "sqlalchemy<2"
>   ```
>   or pin Superset to a build compatible with SQLAlchemy 2.x.
>
> Customize Iceberg checks by exporting `ICEBERG_NAMESPACE` and `ICEBERG_TABLE` before running the script.

### Hadoop & Iceberg references

- HDFS binaries: `/home/dlhnhom2/hadoop/bin/{hdfs,hadoop}`
- Warehouse root: `/warehouse` (HDFS) -> Iceberg catalog: `hdfs:///warehouse/iceberg_test`
- Sample Iceberg table: `hadoop_catalog.default.air_quality`
- Metadata directory probed by health check: `hdfs:///warehouse/iceberg_test/${ICEBERG_NAMESPACE:-default}/${ICEBERG_TABLE:-air_quality}/metadata`
- Spark/Iceberg runtime jars: `/home/dlhnhom2/spark/jars/`

### Python environment

- Python 3.10.12 system interpreter.
- Key packages pinned via `requirements.txt` (PySpark, PyArrow, MLflow, pandas, scikit-learn).
- Virtual environment optional (`python3 -m venv venv && source venv/bin/activate`).

## Data Warehouse Model

Target schema lives in `docs/schema/Sample.sql`, covering:

- `dim_date`, `dim_time`, `dim_location`, `dim_pollutant`
- `fact_air_quality_hourly`
- `fact_city_daily`
- `fact_episode`

All fact tables are designed for Iceberg tables served from the Hadoop catalog. Hourly fact data is the canonical source for downstream aggregations and ML feature builds.

## Analytics Backlog

### 1. City AQI KPI & Benchmark
- **Inputs**: Open-Meteo hourly feed -> `time, aqi, aqi_*` plus pollutant concentrations (`pm2_5`, `pm10`, `ozone`, `no2`, `so2`, `co`, `dust`, `uv_index`, `aerosol_optical_depth`).
- **Transformations**:
  - Normalize timestamps (UTC vs local time zones derived from `configs/locations.json`).
  - Populate hourly warehouse table (`fact_air_quality_hourly`) with completeness flags.
  - Aggregate into daily KPI table (`fact_city_daily`) computing `aqi_daily_max`, hourly counts per US AQI category, `dominant_pollutant_daily_us`, and `data_completeness`.
- **Visuals**:
  - Multi-city AQI line chart (hourly resolution, timezone-aware).
  - Stacked bar (hours per AQI category by day).
  - Calendar heatmap of `aqi_daily_max`.

### 2. Episode Detection (US AQI)
- **Goal**: Detect contiguous pollution episodes per city when AQI >= threshold (default 151) lasting >= configurable hours (default 4).
- **Process**:
  - Use hourly fact table to derive boolean episode flag per timestamp.
  - Collapse runs into `fact_episode_us` (episode id, start/end UTC, duration, peak AQI, hours flagged, dominant pollutant, rule code).
  - Summaries: monthly episode counts, average duration, total hours above threshold.
- **Visuals**:
  - Timeline/Gantt per city highlighting episode blocks.
  - Duration distribution (histogram or violin).
  - Geo scatter map sized by `peak_aqi`.

### 3. PM2.5 Forecasting
- **Target**: Predict `pm2_5` at horizons t+1, t+3, t+24 hours.
- **Feature Store**:
  - Lagged and rolling aggregates for `pm2_5` and co-pollutants (`pm10`, `ozone`, `no2`, `so2`, `co`, `dust`, `aerosol_optical_depth`, `uv_index`).
  - Time-based features (`hour`, `day_of_week`, `month`).
  - Optional expansion with meteorology (wind, temperature) once ingested.
  - Strict leakage prevention (no using contemporaneous `aqi_pm2_5` as a feature).
- **Outputs**:
  - Feature table keyed by (location_key, timestamp, horizon).
  - Labels table with fold identifiers for reproducible CV.
  - Optional feature-importance tracking per model revision.
- **Visuals**:
  - Actual vs forecast line plot (per horizon/city).
  - `y_hat` vs `y` scatter and residual distribution.
  - Feature-importance bar chart.

## Operational Checklist

- [ ] Run `./scripts/health_check.sh` after any system updates.
- [ ] Leave HDFS safe mode if ingest jobs need write access:
  ```bash
  hdfs dfsadmin -safemode leave
  ```
- [ ] Start Spark Thrift Server before BI tooling or Superset dashboards.
- [ ] Resolve Superset vs SQLAlchemy version mismatch (pin SQLAlchemy < 2 or upgrade Superset with compatible dependencies).
- [ ] Configure Superset datasource to Spark Thrift (`hive://dlhnhom2@localhost:10000/default`) once port 10000 is open.
- [ ] Point MLflow tracking URI to desired backend (default is local filesystem).
- [ ] Review Prefect profile (`prefect config view`) and set API/agent targets before orchestrating flows (current profile: `ephemeral` with SQLite backing store).

## Repository Layout

- `configs/locations.json` - seed dimensions for cities (lat/lon, timezone).
- `docs/schema/Sample.sql` - reference DDL for Iceberg tables.
- `scripts/health_check.sh` - consolidated readiness probe.
- `requirements.txt` - Python dependencies (PySpark, data processing, ML).
- `notebooks/` - exploratory workbenches (if any).
- `jobs/` - scheduled ingestion/transformation scripts (placeholder).

## Getting Started (developer workflow)

1. Create or activate a virtualenv (optional).
2. Install Python deps:
   ```bash
   python3 -m pip install -r requirements.txt
   ```
3. Run the health check script (override `ICEBERG_NAMESPACE`/`ICEBERG_TABLE` as needed) and address warnings.
4. Kick off sample Spark job or notebook to populate Iceberg tables.
5. Register Superset datasource and draft dashboards once Spark Thrift is reachable.
6. Set up MLflow tracking server if remote experiment logging is needed.

## Change Log

- 2025-10-12: Added automated health check script, Hadoop catalog diagnostics, Prefect coverage, and consolidated documentation.
