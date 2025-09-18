
# AQ Lakehouse — Air Quality Analytics Platform

This repository implements a data lakehouse for hourly air quality analytics focused on provinces and cities in Vietnam. The README intentionally describes the system, data contracts, constraints, and examples in precise English so an AI or developer can reliably read the spec and generate code or tests.

## Summary

- Purpose: Ingest hourly air quality measurements from the Open-Meteo Air Quality API, store them in an Iceberg-backed lakehouse on HDFS, and provide transformed analytical tables for downstream consumption (dashboards, queries, ML). All ETL is run on demand (no scheduler).
- Coverage: A fixed set of geographic points (latitude/longitude) that map to provinces/cities in Vietnam.
- Timezone: Data are normalized and stored in timezone UTC+7. Source timestamps are converted to UTC+7 during ingestion.

## Technology Stack

- Storage/Cluster: Hadoop Distributed File System (HDFS)
- Compute: Apache Spark (PySpark or Scala Spark as implemented in this repo)
- Table Format: Apache Iceberg
- Catalog: Hadoop Catalog (Iceberg using Hadoop table metadata)
- Visualization: Apache Superset (consumes Iceberg tables via SQL engine)

## Data Source

- Source: Open-Meteo Air Quality API
- API docs: https://open-meteo.com/en/docs/air-quality-api
- Data resolution: hourly
- Geographic scope: a predefined list of coordinates (latitude, longitude) for Vietnamese provinces/cities

Notes: The system queries the API for specific coordinates and does not attempt to discover or enumerate locations dynamically.

## Constraints and Non-Functional Requirements

- No scheduler: ETL jobs are executed only when the corresponding code or script is called manually or from an orchestration tool outside this repo. There is intentionally no cron/airflow/automation setup in the repo.
- Fixed locations: The pipeline accepts a static list of coordinates (and optional labels) defined in configuration. It does not fetch arbitrary locations.
- Idempotency: Re-running an ingestion for the same time window and coordinate should not create duplicate records in Iceberg (use upserts/merge-on-key or partition overwrite semantics).
- Data retention and partitioning strategies should be documented and easy to change in configuration.

## Data Contracts

1. Ingested raw record (per hour, per coordinate):

- source_timestamp_utc: string (ISO 8601 in UTC)
- local_timestamp: string (ISO 8601 converted to UTC+7)
- latitude: float
- longitude: float
- location_id: string (stable id for the coordinate; e.g., `vn_hanoi_1`)
- location_name: string (human label, e.g., `Hanoi - Hoan Kiem`)
- measurements: map<string, double> (keys are pollutant codes such as pm10, pm2_5, no2, o3, so2, co)
- raw_payload: string (optional, the original JSON from the API for audit/debug)

Notes: The pipeline stores a copy of the raw API response (raw_payload) per ingestion run into a raw audit folder (for example `data/raw/`). This raw capture is important for replay, debugging, and building mocked responses for tests.

2. Canonical analytics table (Iceberg) example schema:

- event_time_utc: timestamp
- event_time_local: timestamp (UTC+7)
- location_id: string
- location_name: string
- latitude: double
- longitude: double
- pm2_5: double
- pm10: double
- no2: double
- so2: double
- o3: double
- co: double
- aqi: int (optional — computed)
- ingestion_time: timestamp

Notes: `ingestion_time` should be recorded in UTC and indicates when the pipeline persisted or processed the record (useful for latency and lineage tracking).

Partitioning recommendation: partition by date (event_time_local date) and bucket or partition by location_id for efficient province-level queries.

Primary/uniqueness key (for merges/upserts): (location_id, event_time_local)

## Example configuration

Provide a simple JSON/YAML config that lists coordinates and labels. This config is the single source of truth for locations to ingest.

Example (JSON):

{
	"timezone": "Asia/Bangkok",   # UTC+7
	"locations": [
		{"location_id": "vn_hn_hoankiem", "name": "Hanoi - Hoan Kiem", "lat": 21.0285, "lon": 105.8542},
		{"location_id": "vn_hcm_district1", "name": "Ho Chi Minh - District 1", "lat": 10.7769, "lon": 106.7006}
	],
	"iceberg": {
		"warehouse_path": "/data/iceberg/aq",
		"database": "aq_analytics",
		"table": "hourly_observations"
	},
	"api": {
		"base_url": "https://air-quality-api.open-meteo.com/v1/",
		"timeout_seconds": 20
	}
}

## How the pipeline works (high level)

1. Read the locations config.
2. For each location, call the Open-Meteo Air Quality API for the requested time range (hourly). Convert/normalize timestamps to UTC+7.
3. Parse and validate the hourly measurements. Fill missing pollutant values with nulls.
4. Create a Spark DataFrame with the canonical schema.
5. Write to an Iceberg table using the Hadoop catalog. Use merge/upsert (or partition overwrite with deduplication) to avoid duplicates.

## Running ETL (manual invocation)

This repo purposely contains no scheduler. To run ingestion for a single day or hour, call the ETL entrypoint script or Spark job directly. Example pseudo-steps:

1. Prepare environment (Spark, Python deps from requirements.txt, Hadoop client config pointing to HDFS).
2. Run the ingestion script with parameters: start_time, end_time, config path, and target table.

Suggested CLI signature:

python -m jobs.ingest.run --config configs/locations.json --start 2025-09-17T00:00:00Z --end 2025-09-17T23:00:00Z

Or a Spark-submit invocation (PySpark):

spark-submit --master yarn --deploy-mode cluster jobs/ingest/spark_job.py \
	--config hdfs:///configs/locations.json --start 2025-09-17T00:00:00Z --end 2025-09-17T23:00:00Z

Make sure to set the application timezone conversion to UTC+7 when creating event_time_local.

## AI / Codegen Guidance

This README is written to be precise for AI-driven code generation. When asking an AI to generate code, include these items in the prompt:

- Exact input config shape (show the JSON example above).
- Exact output schema for the Iceberg table (supply the schema block above).
- Idempotency requirement: merging/upserting on (location_id, event_time_local).
- Timezone: Normalize timestamps to UTC+7.
- Edge cases to handle: missing measurements, partial API responses, API rate limits, network timeouts.

Prompt template (example):

"Generate a PySpark ingestion job that reads a locations JSON, calls the Open-Meteo Air Quality API hourly for each coordinate between start and end timestamps, normalizes timestamps to UTC+7, expands measurements to columns (pm2_5, pm10, no2, so2, o3, co), and writes to an Iceberg table (Hadoop catalog) using upsert semantics on (location_id, event_time_local). Include logging, retries, input validation, and tests for schema correctness."

## Edge cases and failure modes

- API rate limits: implement retries with exponential backoff and backpressure (throttle concurrent API calls).
- Missing or null pollutant values: store as nulls and optionally emit a data quality warning metric.
- Duplicate ingestion runs: use merge/upsert semantics or dedupe by key before write.
- Clock skew and DST: Vietnam uses a fixed UTC+7; no DST changes expected, but always convert from source UTC to UTC+7 reliably.

## Tests and Quality

- Add unit tests for the parsers that transform API JSON into the canonical row format.
- Add an integration smoke test that runs the pipeline for a single mocked location and a short time range using recorded API responses.

## Troubleshooting

- HDFS/permissions errors: confirm Hadoop client configs and HDFS user permissions.
- Iceberg table not found: check the Hadoop catalog warehouse path and ensure metadata files exist under the specified table path.
- Timezone mismatch: verify the event_time_local column and any downstream dashboards are using UTC+7.

## Next steps (recommended small additions)

- Provide an example `configs/locations.json` and a small `jobs/ingest/mock_responses/` folder with recorded API responses for tests.
- Add a small `scripts/run_local_ingest.sh` that sets required env vars and calls the ingestion entrypoint for local development.

## License

This repo does not currently include a license file. Add an appropriate license if you intend to share the code publicly.

