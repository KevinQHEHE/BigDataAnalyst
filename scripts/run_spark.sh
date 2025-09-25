#!/usr/bin/env bash
set -euo pipefail

# Unified spark submit helper: run_spark.sh
# Combines submit_yarn.sh, submit_standalone.sh and submit_local.sh
# Usage:
#   bash scripts/run_spark.sh --mode yarn|standalone|local [--rebuild-pyfiles] [--master-url URL] <path-to-job> [job args...]
# Default mode: yarn (production). Use local for fastest developer loop.

show_help() {
  cat <<EOF
Usage: $(basename "$0") [--mode yarn|standalone|local] [--rebuild-pyfiles] [--master-url URL] <job.py> [args...]

Examples:
  bash scripts/run_spark.sh --mode local jobs/bronze/open_meteo_bronze.py --locations configs/locations.json --start 2024-01-01 --end 2024-01-31
  bash scripts/run_spark.sh --mode standalone --master-url spark://khoa-master:7077 jobs/bronze/open_meteo_bronze.py ...
  bash scripts/run_spark.sh --mode yarn jobs/bronze/open_meteo_bronze.py ...
EOF
}

DEFAULT_MODE="yarn"
MODE="$DEFAULT_MODE"
REBUILD_PYFILES=0
MASTER_URL=""

# Simple args parse (stop at first non-option which is the job path)
while [[ ${1:-} == --* ]]; do
  case "$1" in
    --mode)
      MODE="$2"; shift 2;;
    --rebuild-pyfiles)
      REBUILD_PYFILES=1; shift;;
    --master-url)
      MASTER_URL="$2"; shift 2;;
    --help|-h)
      show_help; exit 0;;
    *)
      echo "Unknown option: $1" >&2; show_help; exit 2;;
  esac
done

APP_PY_RAW="${1:-}"; shift || true
if [[ -z "$APP_PY_RAW" ]]; then
  echo "Error: job path required" >&2
  show_help
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

# Load project .env if available
if [[ -f "$ROOT_DIR/.env" ]]; then
  # shellcheck disable=SC1090
  source "$ROOT_DIR/.env"
fi

# Ensure src is importable
export PYTHONPATH="$ROOT_DIR/src:${PYTHONPATH:-}"

# Determine spark-submit command
if [[ -n "${SPARK_SUBMIT:-}" ]]; then
  SPARK_CMD=("$SPARK_SUBMIT")
elif [[ -n "${SPARK_HOME:-}" && -x "${SPARK_HOME}/bin/spark-submit" ]]; then
  SPARK_CMD=("${SPARK_HOME}/bin/spark-submit")
else
  SPARK_CMD=("spark-submit")
fi

# Helper to create py-files zip if needed
create_pyfiles_if_missing() {
  local archive="$ROOT_DIR/dist/dlh_aqi_src.zip"
  mkdir -p "$ROOT_DIR/dist"
  if [[ $REBUILD_PYFILES -eq 1 || ! -f "$archive" ]]; then
    echo "Creating project py-files archive at $archive (contains src/)" >&2
    # Prefer python3, then python, then zip CLI
    local pybin=""
    if command -v python3 >/dev/null 2>&1; then
      pybin=$(command -v python3)
    elif command -v python >/dev/null 2>&1; then
      pybin=$(command -v python)
    fi

    if [[ -n "$pybin" ]]; then
      # use Python to build zip
      "$pybin" - <<PYCODE
import shutil, os, sys
root = os.path.abspath(os.environ.get('ROOT_DIR', '$ROOT_DIR'))
src = os.path.join(root, 'src')
out = os.path.join(root, 'dist', 'dlh_aqi_src')
if not os.path.isdir(src):
    print('Error: src/ not found at', src, file=sys.stderr)
    sys.exit(2)
shutil.make_archive(out, 'zip', src)
print('Wrote', out + '.zip')
PYCODE
    elif command -v zip >/dev/null 2>&1; then
      (cd "$ROOT_DIR/src" && zip -r "$ROOT_DIR/dist/dlh_aqi_src.zip" .) >/dev/null
      echo "Wrote $archive" >&2
    else
      echo "Error: cannot create py-files archive: install python3 or zip, or set SPARK_PYFILES/SPARK_STANDALONE_PYFILES" >&2
      exit 1
    fi
  fi
  echo "$archive"
}

# Mode-specific configuration builders
build_conf_yarn() {
  # minimal tuned for this project; uses env defaults from .env
  local conf=(
    --master yarn
    --deploy-mode client
    --conf spark.ui.enabled=false
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog
    --conf spark.sql.catalog.hadoop_catalog.type=hadoop
    --conf "spark.sql.catalog.hadoop_catalog.warehouse=${WAREHOUSE_URI:-hdfs://khoa-master:9000/warehouse/iceberg}"
    --conf spark.sql.execution.arrow.pyspark.enabled=false
    --conf spark.yarn.maxAppAttempts=1
    --conf spark.dynamicAllocation.enabled=false
    --conf "spark.sql.adaptive.enabled=${SPARK_ENABLE_AQE:-true}"
    --conf "spark.sql.adaptive.coalescePartitions.enabled=${SPARK_AQE_COALESCE:-true}"
  --conf "spark.sql.adaptive.skewJoin.enabled=${SPARK_AQE_SKEW:-true}"
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-200}"
  --conf "spark.hadoop.parquet.compression=${SPARK_HADOOP_PARQUET_COMPRESSION:-SNAPPY}"
  --conf "spark.sql.files.maxPartitionBytes=${SPARK_MAX_PARTITION_BYTES:-33554432}"
  --conf "spark.sql.parquet.compression.codec=${SPARK_PARQUET_COMPRESSION:-snappy}"
    --conf "parquet.block.size=${SPARK_PARQUET_BLOCK_SIZE:-33554432}"
    --conf "parquet.page.size=${SPARK_PARQUET_PAGE_SIZE:-1048576}"
    --conf "spark.sql.session.timeZone=${SPARK_SQL_SESSION_TZ:-UTC}"
    --conf "spark.yarn.submit.file.replication=${SPARK_YARN_FILE_REPLICATION:-1}"
    --conf "spark.yarn.am.memory=${SPARK_YARN_AM_MEMORY:-384m}"
    --conf "spark.yarn.am.memoryOverhead=${SPARK_YARN_AM_OVERHEAD:-128m}"
    --conf "spark.executor.instances=${SPARK_EXECUTOR_INSTANCES:-1}"
  --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES:-1}"
  --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY:-3072m}"
  --conf "spark.executor.memoryOverhead=${SPARK_EXECUTOR_MEMORY_OVERHEAD:-1024m}"
  --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY:-2048m}"
    --conf "spark.executor.memoryFraction=${SPARK_EXECUTOR_MEMORY_FRACTION:-0.6}"
    --conf "spark.storage.memoryFraction=${SPARK_EXECUTOR_MEMORY_STORAGE_FRACTION:-0.5}"
    --conf "spark.serializer=${SPARK_SERIALIZER:-org.apache.spark.serializer.KryoSerializer}"
    --conf "spark.kryo.unsafe=${SPARK_KRYO_UNSAFE:-true}"
  )
  echo "${conf[@]}"
}

build_conf_standalone() {
  local master="${MASTER_URL:-${SPARK_MASTER_URL:-spark://khoa-master:7077}}"
  local conf=(
    --master "$master"
    --deploy-mode client
    --conf spark.ui.enabled=false
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog
    --conf spark.sql.catalog.hadoop_catalog.type=hadoop
    --conf "spark.sql.catalog.hadoop_catalog.warehouse=${WAREHOUSE_URI:-hdfs://khoa-master:9000/warehouse/iceberg}"
    --conf spark.sql.execution.arrow.pyspark.enabled=false
    --conf spark.dynamicAllocation.enabled=false
    --conf "spark.sql.adaptive.enabled=${SPARK_ENABLE_AQE:-true}"
    --conf "spark.sql.adaptive.coalescePartitions.enabled=${SPARK_AQE_COALESCE:-true}"
  --conf "spark.sql.adaptive.skewJoin.enabled=${SPARK_AQE_SKEW:-true}"
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-200}"
  --conf "spark.hadoop.parquet.compression=${SPARK_HADOOP_PARQUET_COMPRESSION:-SNAPPY}"
  --conf "spark.sql.files.maxPartitionBytes=${SPARK_MAX_PARTITION_BYTES:-33554432}"
  --conf "spark.sql.parquet.compression.codec=${SPARK_PARQUET_COMPRESSION:-snappy}"
    --conf "spark.sql.session.timeZone=${SPARK_SQL_SESSION_TZ:-UTC}"
  --conf "spark.executor.instances=${SPARK_EXECUTOR_INSTANCES:-2}"
  --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES:-2}"
  --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY:-3072m}"
  --conf "spark.executor.memoryOverhead=${SPARK_EXECUTOR_MEMORY_OVERHEAD:-1024m}"
  --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY:-2048m}"
    --conf "parquet.block.size=${SPARK_PARQUET_BLOCK_SIZE:-33554432}"
    --conf "parquet.page.size=${SPARK_PARQUET_PAGE_SIZE:-1048576}"
    --conf "spark.executor.memoryFraction=${SPARK_EXECUTOR_MEMORY_FRACTION:-0.6}"
    --conf "spark.storage.memoryFraction=${SPARK_EXECUTOR_MEMORY_STORAGE_FRACTION:-0.5}"
    --conf "spark.serializer=${SPARK_SERIALIZER:-org.apache.spark.serializer.KryoSerializer}"
    --conf "spark.kryo.unsafe=${SPARK_KRYO_UNSAFE:-true}"
    --conf "spark.yarn.submit.file.replication=1"
  )
  echo "${conf[@]}"
}

build_conf_local() {
  local conf=(
    --master local[*]
    --deploy-mode client
    --conf spark.ui.enabled=false
    # Local runs perform writes on the driver; give a larger default to avoid Java heap OOM on Parquet write
    --conf spark.driver.memory=${SPARK_DRIVER_MEMORY:-6g}
    --conf "spark.sql.parquet.compression.codec=${SPARK_PARQUET_COMPRESSION:-snappy}"
  --conf "parquet.block.size=${SPARK_PARQUET_BLOCK_SIZE:-33554432}"
  --conf "parquet.page.size=${SPARK_PARQUET_PAGE_SIZE:-1048576}"
    --conf spark.sql.execution.arrow.pyspark.enabled=false
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS:-200}"
  --conf "spark.hadoop.parquet.compression=${SPARK_HADOOP_PARQUET_COMPRESSION:-SNAPPY}"
  --conf "spark.sql.files.maxPartitionBytes=${SPARK_MAX_PARTITION_BYTES:-33554432}"
    --conf "spark.sql.adaptive.enabled=${SPARK_ENABLE_AQE:-false}"
    --conf "spark.sql.session.timeZone=${SPARK_SQL_SESSION_TZ:-UTC}"
  )
  echo "${conf[@]}"
}

# Build final command depending on mode
MODE_LOWER=$(echo "$MODE" | tr '[:upper:]' '[:lower:]')
case "$MODE_LOWER" in
  yarn)
    IFS=' ' read -r -a CONF <<< "$(build_conf_yarn)"
    # Use SPARK_PYFILES env if set
    PYFILES=()
    if [[ -n "${SPARK_PYFILES:-}" ]]; then
      PYFILES=(--py-files "${SPARK_PYFILES}")
    fi
    ;;
  standalone)
    IFS=' ' read -r -a CONF <<< "$(build_conf_standalone)"
    PYFILES=()
    if [[ -n "${SPARK_STANDALONE_PYFILES:-}" ]]; then
      PYFILES=(--py-files "${SPARK_STANDALONE_PYFILES}")
    else
      PYFILES=(--py-files "$(create_pyfiles_if_missing)")
    fi
    ;;
  local)
    IFS=' ' read -r -a CONF <<< "$(build_conf_local)"
    PYFILES=()
    ;;
  *)
    echo "Unknown mode: $MODE" >&2; exit 2;;
esac

# Final command: all options first, then application and its args
CMD=("${SPARK_CMD[@]}" "${CONF[@]}" "${PYFILES[@]}" "$RESOLVED_APP" "$@")

echo "+ ${CMD[*]}"
exec "${CMD[@]}"
