#!/usr/bin/env bash
set -euo pipefail

# Usage: submit_yarn.sh <path-to-python-job> [args...]
# The script will try the exact path, then workspace-relative, then workspace/jobs/...

APP_PY_RAW="${1:-}"; shift || true
if [[ -z "$APP_PY_RAW" ]]; then
  echo "Usage: $(basename "$0") <path-to-python-job> [args...]" >&2
  exit 2
fi

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

try_paths=("$APP_PY_RAW" "$ROOT_DIR/$APP_PY_RAW" "$ROOT_DIR/jobs/$APP_PY_RAW")
RESOLVED_APP=""
for p in "${try_paths[@]}"; do
  if [[ -f "$p" ]]; then
    RESOLVED_APP="$p"
    break
  fi
done

if [[ -z "$RESOLVED_APP" ]]; then
  echo "Error: job file not found: $APP_PY_RAW" >&2
  echo "Tried: ${try_paths[*]}" >&2
  exit 2
fi

# Ensure PYTHONPATH includes project src so imports like aq_lakehouse.* work
export PYTHONPATH="$ROOT_DIR/src:${PYTHONPATH:-}"

# Determine spark-submit command: prefer SPARK_SUBMIT, else SPARK_HOME/bin/spark-submit, else spark-submit on PATH
if [[ -n "${SPARK_SUBMIT:-}" ]]; then
  SPARK_CMD=("$SPARK_SUBMIT")
elif [[ -n "${SPARK_HOME:-}" && -x "${SPARK_HOME}/bin/spark-submit" ]]; then
  SPARK_CMD=("${SPARK_HOME}/bin/spark-submit")
else
  SPARK_CMD=("spark-submit")
fi

WAREHOUSE_URI=${WAREHOUSE_URI:-hdfs://khoa-master:9000/warehouse/iceberg}

CMD=("${SPARK_CMD[@]}"
  --master yarn
  --deploy-mode client
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop
  --conf spark.sql.catalog.hadoop_catalog.warehouse="$WAREHOUSE_URI"
  --conf spark.yarn.maxAppAttempts=1
  --conf spark.dynamicAllocation.enabled=true
  --conf spark.dynamicAllocation.minExecutors=1
  --conf spark.dynamicAllocation.maxExecutors=6
  "$RESOLVED_APP" "$@"
)

echo "+ ${CMD[*]}"
exec "${CMD[@]}"
