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

# Load environment overrides from project .env if present (non-interactive loader)
if [[ -f "$ROOT_DIR/.env" ]]; then
  # shellcheck disable=SC1090
  source "$ROOT_DIR/.env"
fi

# Provide defaults if vars are unset (right-sized for small monthly batches)
SPARK_EXECUTOR_INSTANCES=${SPARK_EXECUTOR_INSTANCES:-1}
SPARK_EXECUTOR_CORES=${SPARK_EXECUTOR_CORES:-1}
SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-512m}
SPARK_EXECUTOR_MEMORY_OVERHEAD=${SPARK_EXECUTOR_MEMORY_OVERHEAD:-256m}
SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-512m}

# Pre-staged archive (avoid uploading Spark libs each submit)
SPARK_YARN_ARCHIVE=${SPARK_YARN_ARCHIVE:-}
SPARK_YARN_JARS=${SPARK_YARN_JARS:-}
SPARK_PYFILES=${SPARK_PYFILES:-}

# YARN cluster limits (MB) used to cap executor/driver memory to avoid requesting more than cluster allows
YARN_MAX_ALLOCATION_MB=${YARN_MAX_ALLOCATION_MB:-2048}

# AQE / adaptive settings
SPARK_ENABLE_AQE=${SPARK_ENABLE_AQE:-true}
SPARK_AQE_COALESCE=${SPARK_AQE_COALESCE:-true}
SPARK_AQE_SKEW=${SPARK_AQE_SKEW:-true}

# Shuffle partitions default
SPARK_SQL_SHUFFLE_PARTITIONS=${SPARK_SQL_SHUFFLE_PARTITIONS:-2}

# Memory optimization settings for large date ranges
SPARK_EXECUTOR_MEMORY_FRACTION=${SPARK_EXECUTOR_MEMORY_FRACTION:-0.6}
SPARK_EXECUTOR_MEMORY_STORAGE_FRACTION=${SPARK_EXECUTOR_MEMORY_STORAGE_FRACTION:-0.5}
SPARK_SERIALIZER=${SPARK_SERIALIZER:-org.apache.spark.serializer.KryoSerializer}
SPARK_KRYO_UNSAFE=${SPARK_KRYO_UNSAFE:-true}

# Session and YARN AM settings
SPARK_SQL_SESSION_TZ=${SPARK_SQL_SESSION_TZ:-UTC}
SPARK_YARN_FILE_REPLICATION=${SPARK_YARN_FILE_REPLICATION:-1}
SPARK_YARN_AM_MEMORY=${SPARK_YARN_AM_MEMORY:-384m}
SPARK_YARN_AM_OVERHEAD=${SPARK_YARN_AM_OVERHEAD:-128m}



# Helper: convert human memory (e.g. 3g, 768m) to integer MB
to_mb() {
  local v="$1"
  if [[ "$v" =~ ^([0-9]+)[gG]$ ]]; then
    echo $(( ${BASH_REMATCH[1]} * 1024 ))
  elif [[ "$v" =~ ^([0-9]+)[mM]$ ]]; then
    echo ${BASH_REMATCH[1]}
  elif [[ "$v" =~ ^[0-9]+$ ]]; then
    echo "$v"
  else
    echo "$v"
  fi
}
## Perform memory capping BEFORE building CONF/CMD so options are consistent
if [[ "$YARN_MAX_ALLOCATION_MB" =~ ^[0-9]+$ ]]; then
  exec_mem_mb=$(to_mb "${SPARK_EXECUTOR_MEMORY}")
  exec_over_mb=$(to_mb "${SPARK_EXECUTOR_MEMORY_OVERHEAD}")
  total_exec_mb=$(( exec_mem_mb + exec_over_mb ))
  if (( total_exec_mb > YARN_MAX_ALLOCATION_MB )); then
    allowed_exec_mem_mb=$(( YARN_MAX_ALLOCATION_MB - exec_over_mb ))
    if (( allowed_exec_mem_mb < 128 )); then
      echo "ERROR: requested executor memory (${total_exec_mb}MB) exceeds YARN max (${YARN_MAX_ALLOCATION_MB}MB) and cannot be reduced safely" >&2
      exit 1
    fi
    echo "Warning: capping executor memory ${SPARK_EXECUTOR_MEMORY} (+${SPARK_EXECUTOR_MEMORY_OVERHEAD}) -> ${allowed_exec_mem_mb}MB to fit YARN max ${YARN_MAX_ALLOCATION_MB}MB"
    SPARK_EXECUTOR_MEMORY="${allowed_exec_mem_mb}m"
  fi
fi

# Build CONF array (all spark options must come BEFORE the application path)
CONF=(
  --master yarn
  --deploy-mode client
  --conf spark.ui.enabled=false
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop
  --conf spark.sql.catalog.hadoop_catalog.warehouse="$WAREHOUSE_URI"
  --conf spark.sql.execution.arrow.pyspark.enabled=false
  --conf spark.yarn.maxAppAttempts=1
  --conf spark.dynamicAllocation.enabled=false
  --conf "spark.sql.adaptive.enabled=${SPARK_ENABLE_AQE}"
  --conf "spark.sql.adaptive.coalescePartitions.enabled=${SPARK_AQE_COALESCE}"
  --conf "spark.sql.adaptive.skewJoin.enabled=${SPARK_AQE_SKEW}"
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS}"
  --conf "spark.sql.session.timeZone=${SPARK_SQL_SESSION_TZ}"
  --conf "spark.yarn.submit.file.replication=${SPARK_YARN_FILE_REPLICATION}"
  --conf "spark.yarn.am.memory=${SPARK_YARN_AM_MEMORY}"
  --conf "spark.yarn.am.memoryOverhead=${SPARK_YARN_AM_OVERHEAD}"
  --conf "spark.executor.instances=${SPARK_EXECUTOR_INSTANCES}"
  --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES}"
  --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}"
  --conf "spark.executor.memoryOverhead=${SPARK_EXECUTOR_MEMORY_OVERHEAD}"
  --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY}"
  --conf "spark.executor.memoryFraction=${SPARK_EXECUTOR_MEMORY_FRACTION}"
  --conf "spark.storage.memoryFraction=${SPARK_EXECUTOR_MEMORY_STORAGE_FRACTION}"
  --conf "spark.serializer=${SPARK_SERIALIZER}"
  --conf "spark.kryo.unsafe=${SPARK_KRYO_UNSAFE}"
)

# Inject pre-staged archive if provided
if [[ -n "${SPARK_YARN_ARCHIVE}" ]]; then
  CONF+=(--conf "spark.yarn.archive=${SPARK_YARN_ARCHIVE}")
fi

# Inject pre-staged jars if provided
if [[ -n "${SPARK_YARN_JARS}" ]]; then
  CONF+=(--conf "spark.yarn.jars=${SPARK_YARN_JARS}")
fi

# Build PYFILES array
PYFILES=()
if [[ -n "${SPARK_PYFILES}" ]]; then
  PYFILES=(--py-files "${SPARK_PYFILES}")
fi

# Final command: all options first, then application and its args
CMD=("${SPARK_CMD[@]}" "${CONF[@]}" "${PYFILES[@]}" "$RESOLVED_APP" "$@")

echo "+ ${CMD[*]}"
exec "${CMD[@]}"
