#!/usr/bin/env bash
set -euo pipefail

# Lightweight local submit helper for development & testing.
# Intention: run PySpark jobs locally (local[*]) with minimal overhead.
# This is for fast, interactive runs only. Do NOT use in production clusters.

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

# Ensure project code is importable without zipping
export PYTHONPATH="$ROOT_DIR/src:${PYTHONPATH:-}"

# Locate spark-submit
if [[ -n "${SPARK_SUBMIT:-}" ]]; then
  SPARK_CMD=("$SPARK_SUBMIT")
elif [[ -n "${SPARK_HOME:-}" && -x "${SPARK_HOME}/bin/spark-submit" ]]; then
  SPARK_CMD=("${SPARK_HOME}/bin/spark-submit")
else
  SPARK_CMD=("spark-submit")
fi

# Local-friendly defaults tuned for fast inner-loop:
DRIVER_MEMORY=${SPARK_DRIVER_MEMORY:-2g}
SHUFFLE_PARTS=${SPARK_SQL_SHUFFLE_PARTITIONS:-4}
AQE_ENABLED=${SPARK_ENABLE_AQE:-false}

CONF=(
  --master local[*]
  --deploy-mode client
  --conf spark.ui.enabled=false
  --conf spark.driver.memory=${DRIVER_MEMORY}
  --conf spark.sql.execution.arrow.pyspark.enabled=false
  --conf "spark.sql.shuffle.partitions=${SHUFFLE_PARTS}"
  --conf "spark.sql.adaptive.enabled=${AQE_ENABLED}"
  --conf "spark.sql.session.timeZone=${SPARK_SQL_SESSION_TZ:-UTC}"
)

# Final command: options then app
CMD=("${SPARK_CMD[@]}" "${CONF[@]}" "$RESOLVED_APP" "$@")

echo "+ ${CMD[*]}"
exec "${CMD[@]}"
