#!/bin/bash
# Hourly AQI Pipeline Runner on YARN
# This script is designed to be scheduled via cron or Prefect shell worker

set -e

cd "$(dirname "$0")/.."

LOG_DIR="logs"
mkdir -p "$LOG_DIR"

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="$LOG_DIR/hourly_pipeline_$TIMESTAMP.log"

echo "==========================================" | tee "$LOG_FILE"
echo "Hourly AQI Pipeline - $(date)" | tee -a "$LOG_FILE"
echo "==========================================" | tee -a "$LOG_FILE"

# Run via spark-submit wrapper (YARN mode)
bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourly 2>&1 | tee -a "$LOG_FILE"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ Pipeline completed successfully" | tee -a "$LOG_FILE"
else
    echo "✗ Pipeline failed with exit code $EXIT_CODE" | tee -a "$LOG_FILE"
fi

exit $EXIT_CODE
