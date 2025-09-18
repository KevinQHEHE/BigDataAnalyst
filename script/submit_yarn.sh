#!/usr/bin/env bash
set -euo pipefail
APP_PY="$1"; shift || true

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
SRC_DIR="$PROJECT_ROOT/src"

SPARK_HOME=${SPARK_HOME:-/home/dlhnhom2/spark}
WAREHOUSE_URI=${WAREHOUSE_URI:-hdfs://khoa-master:9000/warehouse/iceberg_test}

# Ensure local packages under src/ are importable from driver and executors
export PYTHONPATH="${PYTHONPATH:+$PYTHONPATH:}$SRC_DIR"

"$SPARK_HOME"/bin/spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop \
  --conf spark.sql.catalog.hadoop_catalog.warehouse="$WAREHOUSE_URI" \
  --conf spark.yarn.maxAppAttempts=1 \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.minExecutors=1 \
  --conf spark.dynamicAllocation.maxExecutors=6 \
  "$APP_PY" "$@"
