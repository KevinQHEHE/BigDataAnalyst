#!/usr/bin/env bash
set -euo pipefail

PYTHON_BIN=${PYTHON_BIN:-python3}

if $PYTHON_BIN -m jupyterlab --version >/dev/null 2>&1; then
  DRIVER_OPTS="-m jupyterlab --no-browser"
else
  DRIVER_OPTS="-m notebook --no-browser"
fi

PYSPARK_DRIVER_PYTHON=${PYTHON_BIN} \
PYSPARK_DRIVER_PYTHON_OPTS="${DRIVER_OPTS}" \
${SPARK_HOME:-/home/dlhnhom2/spark}/bin/pyspark --master yarn \
  --conf spark.pyspark.python=${PYTHON_BIN} \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop \
  --conf spark.sql.catalog.hadoop_catalog.warehouse="${WAREHOUSE_URI:-hdfs://khoa-master:9000/warehouse/iceberg}" \
  --conf spark.sql.catalogImplementation=in-memory
