# Gold Layer Documentation

Complete Gold layer with **4 dimension tables** and **3 fact tables** following star schema design.

## Quick Start

```bash
# Load all dimensions + transform all facts
bash scripts/spark_submit.sh jobs/gold/run_gold_pipeline.py -- --mode all

# Load only dimensions
bash scripts/spark_submit.sh jobs/gold/run_gold_pipeline.py -- --mode dims

# Transform only facts  
bash scripts/spark_submit.sh jobs/gold/run_gold_pipeline.py -- --mode facts
```

## Results Summary

**✅ ALL ACCEPTANCE CRITERIA VALIDATED**

- **Hourly Fact**: 47,088 records | AQI ∈ [0-500] | 6 pollutants
- **Daily Fact**: 1,962 records | One row per (location, date)
- **Episodes**: 396 detected | Duration 4-265h | AQI ≥ 151
- **Pipeline Time**: 24.7s (full execution)

## Fact Tables

### 1. fact_air_quality_hourly (47K records)

Enriched hourly measurements with dominant pollutant & data completeness.

```bash
bash scripts/spark_submit.sh jobs/gold/transform_fact_hourly.py -- --mode full
```

**Enrichments:**
- `dominant_pollutant`: argmax(aqi_pm25, aqi_pm10, aqi_o3, aqi_no2, aqi_so2, aqi_co)
- `data_completeness`: (non-null pollutants / 10) * 100%
- `record_id`: UUID

### 2. fact_city_daily (1.9K records)

Daily city aggregates with AQI category hour counts.

```bash
bash scripts/spark_submit.sh jobs/gold/transform_fact_daily.py -- --mode full
```

**AQI Categories:**
- Good (0-50), Moderate (51-100), USG (101-150)
- Unhealthy (151-200), Very Unhealthy (201-300), Hazardous (301+)

### 3. fact_episode (396 episodes)

High AQI episode detection (≥151 for ≥4 consecutive hours).

```bash
# Default: AQI >= 151, duration >= 4h
bash scripts/spark_submit.sh jobs/gold/detect_episodes.py -- --mode full

# Custom thresholds
bash scripts/spark_submit.sh jobs/gold/detect_episodes.py -- \
  --mode full --aqi-threshold 200 --min-hours 6
```

## Validation

```bash
bash scripts/spark_submit.sh scripts/validate_gold_ac.py
```

- ✅ AC1: AQI range [0-500], pollutants {pm25, pm10, o3, no2, so2, co}
- ✅ AC2: Unique (location, date) pairs, no duplicates
- ✅ AC3: Episodes ≥ 4h duration, AQI ≥ 151
- ✅ AC4: All dimension joins successful

