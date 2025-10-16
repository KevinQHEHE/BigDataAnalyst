"""Prefect Flows for DLH-AQI Pipeline.

This package contains Prefect orchestration flows for the complete AQI data pipeline:
- Bronze: Data ingestion from Open-Meteo API
- Silver: Data transformation and enrichment
- Gold: Dimensional modeling and aggregation

All flows are designed to run on YARN via spark_submit.sh wrapper.
"""

__version__ = "1.0.0"

__all__ = [
    "bronze_flow",
    "silver_flow",
    "gold_flow",
    "full_pipeline_flow",
    "backfill_flow",
    "spark_context",
    "deploy"
]
