# Air Quality Lakehouse - Production Data Pipeline

**Lakehouse AQI** is a production-grade data pipeline for air quality analytics built on Apache Iceberg, PySpark 4.0, and HDFS. The pipeline ingests hourly air quality data from Open-Meteo API, transforms it through Bronze → Silver → Gold layers, and provides comprehensive data quality validation.

## Features

- **Multi-layer Architecture**: Bronze (raw), Silver (cleaned), Gold (star schema) data layers
- **Apache Iceberg**: ACID transactions, schema evolution, time travel
- **Comprehensive Validation**: Automated data quality checks for all layers
- **Prefect-Ready**: Task wrappers for orchestration and scheduling
- **Production-Tested**: Running on YARN cluster with 47K+ hourly records

## Quick Start

```bash
# Run full pipeline with validation
bash scripts/run_full_pipeline_with_validation.sh --mode full

# Or run individual layers
bash scripts/run_ingest_bronze.sh --mode full          # Bronze ingestion
bash scripts/run_silver_pipeline.sh --mode full        # Silver transformation
bash scripts/run_gold_pipeline.sh --mode all           # Gold transformation + dims

# Validate data quality
python3 scripts/validate/validate_all.py               # All layers
python3 scripts/validate/validate_bronze.py            # Bronze only
python3 scripts/validate/validate_silver.py            # Silver only
python3 scripts/validate/validate_gold.py              # Gold only
```

## Documentation

- **[Validation Framework](docs/validation/README.md)**: Comprehensive guide to data quality validation
- **[Quick Reference](docs/validation/QUICKSTART.md)**: Common validation commands
- **[Bronze Layer](docs/ingest/bronze.md)**: Raw data ingestion
- **[Silver Layer](docs/ingest/silver.md)**: Data cleaning and transformation
- **[Gold Layer](docs/ingest/gold.md)**: Star schema and business logic
- **[Schema Definition](docs/schema/full-schema.sql)**: Complete table schemas

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

## Data Architecture

### Pipeline Layers

```
┌─────────────────────────────────────────────────────────────────┐
│                        GOLD LAYER (Star Schema)                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐          │
│  │  Dimensions  │  │    Facts     │  │   Episodes   │          │
│  │ - location   │  │ - hourly     │  │ - detection  │          │
│  │ - pollutant  │  │ - daily      │  │ - analytics  │          │
│  │ - time       │  │ - enriched   │  │              │          │
│  │ - date       │  │              │  │              │          │
│  └──────────────┘  └──────────────┘  └──────────────┘          │
└─────────────────────────────────────────────────────────────────┘
                            ▲
                            │ transform_fact_*.py
                            │ load_dim_*.py
┌─────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (Cleaned Data)                   │
│                  air_quality_hourly_clean                        │
│  - Standardized AQI calculation                                 │
│  - Data quality checks (deduplication, null handling)           │
│  - Transformed columns (date_key, time_key)                     │
│  - Partitioned by (date_utc, location_key)                      │
└─────────────────────────────────────────────────────────────────┘
                            ▲
                            │ transform_bronze_to_silver.py
                            │
┌─────────────────────────────────────────────────────────────────┐
│                   BRONZE LAYER (Raw Ingestion)                   │
│                  open_meteo_hourly                               │
│  - Raw API data from Open-Meteo                                 │
│  - Minimal transformations                                      │
│  - Full history retention                                       │
│  - Partitioned by (date_utc, location_key)                      │
└─────────────────────────────────────────────────────────────────┘
                            ▲
                            │ ingest_bronze.py
                            │
                    ┌───────────────┐
                    │  Open-Meteo   │
                    │  Air Quality  │
                    │      API      │
                    └───────────────┘
```

### Data Quality Framework

Each layer includes comprehensive validation:

- **Bronze**: Schema compliance, duplicates, null values, AQI ranges, data freshness, partitioning
- **Silver**: Transformations, data quality, consistency with Bronze (≥90% retention)
- **Gold**: Dimensions completeness, fact table business rules, referential integrity

Run validations after each pipeline execution:
```bash
python3 scripts/validate/validate_all.py
```

See [Validation Framework](docs/validation/README.md) for details.

## Data Warehouse Model

Target schema lives in `docs/schema/full-schema.sql`, covering:

- **Dimensions**: `dim_date`, `dim_time`, `dim_location`, `dim_pollutant`
- **Facts**: `fact_air_quality_hourly`, `fact_air_quality_daily`, `fact_aqi_episode`

All tables are Apache Iceberg format with PARQUET storage and ZSTD compression. Hourly fact data is the canonical source for downstream aggregations and ML feature builds.

### Current Data Volumes

| Layer | Table | Records | Status |
|-------|-------|---------|--------|
| Bronze | open_meteo_hourly | 47,088 | ✅ Validated |
| Silver | air_quality_hourly_clean | 47,088 | ✅ Validated |
| Gold | dim_location | 3 | ✅ Validated |
| Gold | dim_pollutant | 10 | ✅ Validated |
| Gold | dim_time | 24 | ✅ Validated |
| Gold | dim_date | 654 | ✅ Validated |
| Gold | fact_air_quality_hourly | 47,088 | ✅ Validated |
| Gold | fact_air_quality_daily | 1,962 | ✅ Validated |
| Gold | fact_aqi_episode | 396 | ✅ Validated |

## Analytics Backlog

### 1. City AQI KPI & Benchmark
- **Inputs**: Open-Meteo hourly feed → `time, aqi, aqi_*` plus pollutant concentrations
- **Status**: ✅ **COMPLETED**
  - Hourly fact table with completeness flags
  - Daily aggregates with AQI category counts
  - Dominant pollutant tracking
- **Visuals** (Pending):
  - Multi-city AQI line chart (hourly resolution, timezone-aware)
  - Stacked bar (hours per AQI category by day)
  - Calendar heatmap of `aqi_daily_max`

### 2. Episode Detection (US AQI)
- **Goal**: Detect pollution episodes when AQI ≥ 151 lasting ≥ 4 hours
- **Status**: ✅ **COMPLETED**
  - Episode detection using window functions
  - `fact_aqi_episode` table with 396 episodes detected
  - Configurable thresholds (AQI, duration)
- **Visuals** (Pending):
  - Timeline/Gantt per city highlighting episode blocks
  - Duration distribution (histogram or violin)
  - Geo scatter map sized by `peak_aqi`

### 3. PM2.5 Forecasting
- **Target**: Predict `pm2_5` at horizons t+1, t+3, t+24 hours
- **Status**: 🔄 **IN PROGRESS**
  - Feature store design (lagged, rolling aggregates)
  - Time-based features (hour, day_of_week, month)
  - Leakage prevention controls
- **Next Steps**:
  - Create feature table keyed by (location_key, timestamp, horizon)
  - Labels table with fold identifiers for reproducible CV
  - Model training and evaluation pipeline

## Pipeline Operations

### Full Pipeline Execution
```bash
# Run complete pipeline with validation
bash scripts/run_full_pipeline_with_validation.sh --mode full

# Incremental updates only
bash scripts/run_full_pipeline_with_validation.sh --mode incremental
```

### Individual Layer Execution
```bash
# Bronze: Ingest raw data from Open-Meteo API
bash scripts/run_ingest_bronze.sh --mode full

# Silver: Clean and transform Bronze data
bash scripts/run_silver_pipeline.sh --mode full

# Gold: Load dimensions and transform facts
bash scripts/run_gold_pipeline.sh --mode all        # All (dims + facts)
bash scripts/run_gold_pipeline.sh --mode dims       # Dimensions only
bash scripts/run_gold_pipeline.sh --mode facts      # Facts only
```

### Data Quality Validation
```bash
# Validate all layers
python3 scripts/validate/validate_all.py

# Validate specific layer
python3 scripts/validate/validate_bronze.py
python3 scripts/validate/validate_silver.py
python3 scripts/validate/validate_gold.py

# Export results to JSON
python3 scripts/validate/validate_all.py --output results.json
```

### Prefect Integration
```bash
# Run validation flow
python flows/validation_flow.py

# Deploy to Prefect server
prefect deployment build flows/validation_flow.py:validate_data_quality -n daily-validation
prefect deployment apply validate_data_quality-deployment.yaml
```

## Operational Checklist

- [x] Bronze ingestion pipeline implemented and validated
- [x] Silver transformation pipeline implemented and validated
- [x] Gold dimensions loaded (location, pollutant, time, date)
- [x] Gold facts implemented (hourly, daily, episode)
- [x] Comprehensive validation framework for all layers
- [x] Prefect-ready task wrappers
- [x] Documentation for all layers and validation
- [ ] Run `./scripts/health_check.sh` after system updates
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
