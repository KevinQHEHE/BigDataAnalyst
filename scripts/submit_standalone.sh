#!/usr/bin/env bash
set -euo pipefail

# Usage: submit_standalone.sh <path-to-python-job> [args...]
# Example:
#   bash scripts/submit_standalone.sh jobs/bronze/open_meteo_bronze.py --locations configs/locations.json --start 2024-01-01 --end 2024-01-31
#
# This script targets a Spark Standalone cluster (master URL like spark://host:7077) and
# prefers static executors (no dynamic allocation) so workers can be pre-warmed and
# submits are near-instant. It will package `src/` into a zip under `dist/` once and reuse it
# as --py-files so executors receive project code quickly.

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

# Determine spark-submit command
if [[ -n "${SPARK_SUBMIT:-}" ]]; then
  SPARK_CMD=("$SPARK_SUBMIT")
elif [[ -n "${SPARK_HOME:-}" && -x "${SPARK_HOME}/bin/spark-submit" ]]; then
  SPARK_CMD=("${SPARK_HOME}/bin/spark-submit")
else
  SPARK_CMD=("spark-submit")
fi

# Load environment overrides from project .env if present
if [[ -f "$ROOT_DIR/.env" ]]; then
  # shellcheck disable=SC1090
  source "$ROOT_DIR/.env"
fi

# Default master URL (override via SPARK_MASTER_URL env)
SPARK_MASTER_URL=${SPARK_MASTER_URL:-"spark://khoa-master:7077"}

# Standalone-specific default sensible for quick local-like runs
SPARK_EXECUTOR_INSTANCES=${SPARK_EXECUTOR_INSTANCES:-2}
SPARK_EXECUTOR_CORES=${SPARK_EXECUTOR_CORES:-2}
SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-1536m}
SPARK_EXECUTOR_MEMORY_OVERHEAD=${SPARK_EXECUTOR_MEMORY_OVERHEAD:-512m}
SPARK_DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-1024m}

# Adaptive/other toggles (we recommend disabling dynamic allocation for low-latency)
SPARK_ENABLE_AQE=${SPARK_ENABLE_AQE:-true}
SPARK_AQE_COALESCE=${SPARK_AQE_COALESCE:-true}
SPARK_AQE_SKEW=${SPARK_AQE_SKEW:-true}

# Shuffle partitions default (small by default to avoid costly shuffles on small runs)
SPARK_SQL_SHUFFLE_PARTITIONS=${SPARK_SQL_SHUFFLE_PARTITIONS:-4}

# Optional pre-staged pyfiles/archive (use if you've pre-uploaded the zip to a shared FS)
SPARK_STANDALONE_PYFILES=${SPARK_STANDALONE_PYFILES:-}

# Build CONF array (all spark options must come BEFORE the application path)
CONF=(
  --master "$SPARK_MASTER_URL"
  --deploy-mode client
  --conf spark.ui.enabled=false
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop
  --conf "spark.sql.catalog.hadoop_catalog.warehouse=${WAREHOUSE_URI:-hdfs://khoa-master:9000/warehouse/iceberg}"
  --conf spark.sql.execution.arrow.pyspark.enabled=false
  --conf spark.dynamicAllocation.enabled=false
  --conf "spark.sql.adaptive.enabled=${SPARK_ENABLE_AQE}"
  --conf "spark.sql.adaptive.coalescePartitions.enabled=${SPARK_AQE_COALESCE}"
  --conf "spark.sql.adaptive.skewJoin.enabled=${SPARK_AQE_SKEW}"
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS}"
  --conf "spark.sql.session.timeZone=${SPARK_SQL_SESSION_TZ:-UTC}"
  --conf "spark.executor.instances=${SPARK_EXECUTOR_INSTANCES}"
  --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES}"
  --conf "spark.executor.memory=${SPARK_EXECUTOR_MEMORY}"
  --conf "spark.executor.memoryOverhead=${SPARK_EXECUTOR_MEMORY_OVERHEAD}"
  --conf "spark.driver.memory=${SPARK_DRIVER_MEMORY}"
  --conf "spark.executor.memoryFraction=${SPARK_EXECUTOR_MEMORY_FRACTION:-0.6}"
  --conf "spark.storage.memoryFraction=${SPARK_EXECUTOR_MEMORY_STORAGE_FRACTION:-0.5}"
  --conf "spark.serializer=${SPARK_SERIALIZER:-org.apache.spark.serializer.KryoSerializer}"
  --conf "spark.kryo.unsafe=${SPARK_KRYO_UNSAFE:-true}"
  --conf "spark.yarn.submit.file.replication=1"
)

# Build PYFILES array: prefer pre-staged archive, else create one locally under dist/
PYFILES=()
if [[ -n "$SPARK_STANDALONE_PYFILES" ]]; then
  PYFILES=(--py-files "$SPARK_STANDALONE_PYFILES")
else
  mkdir -p "$ROOT_DIR/dist"
  ARCHIVE="$ROOT_DIR/dist/dlh_aqi_src.zip"
  if [[ ! -f "$ARCHIVE" ]]; then
    echo "Creating project py-files archive at $ARCHIVE (contains src/)" >&2
    # Prefer python3, then python. If neither is available, fall back to zip CLI.
    PY_BIN=""
    if command -v python3 >/dev/null 2>&1; then
      PY_BIN="$(command -v python3)"
    elif command -v python >/dev/null 2>&1; then
      PY_BIN="$(command -v python)"
    fi

    if [[ -n "$PY_BIN" ]]; then
      # Use Python's shutil.make_archive to create a deterministic zip of src/
      "$PY_BIN" - <<PYCODE
import shutil, os, sys
root = os.path.abspath(os.environ.get('ROOT_DIR', '$ROOT_DIR'))
src = os.path.join(root, 'src')
out = os.path.join(root, 'dist', 'dlh_aqi_src')
if not os.path.isdir(src):
    print('Error: src/ directory not found at', src, file=sys.stderr)
    sys.exit(2)
shutil.make_archive(out, 'zip', src)
print('Wrote', out + '.zip')
PYCODE
    elif command -v zip >/dev/null 2>&1; then
      # Use zip CLI as a fallback (run from src to avoid nested paths)
      echo "Creating zip archive using zip CLI" >&2
      (cd "$ROOT_DIR/src" && zip -r "$ROOT_DIR/dist/dlh_aqi_src.zip" .) >/dev/null
      echo "Wrote $ARCHIVE" >&2
    else
      echo "Error: neither python3/python nor 'zip' is available to create $ARCHIVE" >&2
      echo "Install python3 or zip, or set SPARK_STANDALONE_PYFILES to a pre-built archive." >&2
      exit 1
    fi
  fi
  PYFILES=(--py-files "$ARCHIVE")
fi

# Final command: all options first, then application and its args
CMD=("${SPARK_CMD[@]}" "${CONF[@]}" "${PYFILES[@]}" "$RESOLVED_APP" "$@")

echo "+ ${CMD[*]}"
exec "${CMD[@]}"
