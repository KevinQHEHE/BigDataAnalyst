#!/usr/bin/env bash
# Helper script to run bronze ingestion

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# Build command with all args after --
exec bash "$PROJECT_ROOT/scripts/spark_submit.sh" -- "$PROJECT_ROOT/ingest/bronze/ingest_bronze.py" "$@"
