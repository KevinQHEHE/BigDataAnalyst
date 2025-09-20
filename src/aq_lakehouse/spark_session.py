from pyspark.sql import SparkSession
import os

WAREHOUSE_URI = os.getenv("WAREHOUSE_URI", "hdfs://khoa-master:9000/warehouse/iceberg")

def build(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
        .config("spark.sql.catalog.hadoop_catalog.warehouse", WAREHOUSE_URI)
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
