"""Spark session builder for Iceberg + Hadoop catalog, YARN-ready.

Design notes:
- Uses environment variable WAREHOUSE_URI or default to HDFS path from project instructions.
- Does NOT set .master() by default to keep it submit-friendly. A `mode` flag allows local testing.
"""
from typing import Optional
import os
from pyspark.sql import SparkSession

WAREHOUSE_URI = os.getenv("WAREHOUSE_URI", "hdfs://khoa-master:9000/warehouse/iceberg")


def build(app_name: str = "lakehouse_aqi", mode: Optional[str] = None) -> SparkSession:
    """Build a SparkSession configured for Iceberg Hadoop catalog.

    Args:
        app_name: Spark application name.
        mode: Optional runtime mode: None = production (no master set), 'local' = local[*] for dev/tests.

    Returns:
        SparkSession
    """
    builder = SparkSession.builder.appName(app_name)

    if mode == "local":
        # Local-only for developer tests
        builder = builder.master("local[2]")

    # Iceberg Hadoop catalog wiring (see project instructions)
    builder = (
        builder
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
        .config("spark.sql.catalog.hadoop_catalog.warehouse", WAREHOUSE_URI)
        # keep catalogImplementation in-memory to avoid Hive metastore by default
        .config("spark.sql.catalogImplementation", "in-memory")
        # Disable Arrow by default for safety unless enabled by env
        .config("spark.sql.execution.arrow.pyspark.enabled", os.getenv("PYSPARK_ARROW_ENABLED", "false"))
    )

    # Optional: enable dynamic allocation and AQE sensible defaults when running on YARN
    if os.getenv("ENABLE_YARN_DEFAULTS", "true").lower() in ("1", "true", "yes"):
        builder = (
            builder
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.minExecutors", os.getenv("SPARK_DYN_MIN", "1"))
            .config("spark.dynamicAllocation.maxExecutors", os.getenv("SPARK_DYN_MAX", "50"))
            .config("spark.sql.adaptive.enabled", os.getenv("SPARK_AQE_ENABLED", "true"))
        )

    return builder.getOrCreate()
