#!/usr/bin/env bash
set -euo pipefail

# Wrapper to build a spark-submit command for YARN with sensible defaults for AQ jobs.
# Usage: ./scripts/spark_submit.sh --dry-run --mode yarn -- <app and args>

DRY_RUN=false
MODE="yarn"
ARGS=()

while [[ "$#" -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --)
      shift
      break
      ;;
    *)
      ARGS+=("$1")
      shift
      ;;
  esac
done

# Defaults tuned for YARN
SPARK_CONF=(
  "--conf" "spark.dynamicAllocation.enabled=true"
  "--conf" "spark.sql.adaptive.enabled=true"
  "--conf" "spark.dynamicAllocation.minExecutors=1"
  "--conf" "spark.dynamicAllocation.maxExecutors=50"
  "--conf" "spark.yarn.preserve.staging.files=false"
  "--conf" "spark.yarn.archive=hdfs://khoa-master:9000/user/dlhnhom2/spark-libs.zip"
  "--conf" "spark.eventLog.compress=true"
  "--conf" "spark.cleaner.referenceTracking.cleanCheckpoints=true"
)

CMD=(spark-submit)
if [[ "${MODE:-}" == "yarn" ]]; then
  CMD+=("--master" "yarn")
fi

CMD+=("${SPARK_CONF[@]}")

if [[ "$DRY_RUN" == true ]]; then
  echo "DRY RUN spark-submit command:"
  printf '%s ' "${CMD[@]}"
  echo
  if [[ ${#ARGS[@]} -gt 0 ]]; then
    echo "Application and args: ${ARGS[*]}"
  fi
  exit 0
fi

# Execute the command - pass both ARGS (before --) and remaining args after --
exec "${CMD[@]}" "${ARGS[@]:-}" "$@"
