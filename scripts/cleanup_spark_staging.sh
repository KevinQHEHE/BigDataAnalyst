#!/bin/bash
# Quick cleanup of all Spark staging directories
# Usage: bash scripts/cleanup_spark_staging_all.sh [--dry-run]

set -euo pipefail

DRY_RUN=false
if [[ "${1:-}" == "--dry-run" ]]; then
  DRY_RUN=true
fi

STAGING_DIR="/user/${USER}/.sparkStaging"

echo "=========================================="
echo "Spark Staging Full Cleanup"
echo "=========================================="
echo "Staging directory: $STAGING_DIR"
echo "Dry run: $DRY_RUN"
echo "=========================================="

# Check if staging dir exists
if ! hdfs dfs -test -d "$STAGING_DIR" 2>/dev/null; then
  echo "Staging directory does not exist. Nothing to clean."
  exit 0
fi

# Show current usage
echo ""
echo "Current staging directory usage:"
hdfs dfs -du -s -h "$STAGING_DIR"
echo ""

# List all staging dirs
echo "Staging directories to remove:"
hdfs dfs -ls "$STAGING_DIR" 2>/dev/null | grep application_ | awk '{print $8}' | while read dir; do
  SIZE=$(hdfs dfs -du -s "$dir" 2>/dev/null | awk '{print $1}' || echo "0")
  SIZE_MB=$((SIZE / 1024 / 1024))
  echo "  [${SIZE_MB}MB] $dir"
done

echo ""
if [ "$DRY_RUN" = true ]; then
  echo "DRY RUN: Would remove all staging directories above"
  echo "Run without --dry-run to actually remove them"
else
  echo "Removing all staging directories..."
  if hdfs dfs -rm -r -skipTrash "$STAGING_DIR"/* 2>/dev/null; then
    echo "✓ All staging directories removed"
  else
    echo "✗ Failed to remove some directories (they may not exist or be in use)"
  fi
  
  echo ""
  echo "Current staging directory usage:"
  hdfs dfs -du -s -h "$STAGING_DIR" 2>/dev/null || echo "0 B (empty)"
fi

echo "=========================================="
