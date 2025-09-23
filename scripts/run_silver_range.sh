#!/usr/bin/env bash
set -euo pipefail

# Usage examples:
# ./scripts/run_silver_range.sh --start 2024-01-01T00:00:00 --end 2025-09-30T23:00:00
# ./scripts/run_silver_range.sh --start 2024-08-01T00:00:00 --end 2024-08-31T23:00:00 --mode replace
#
# This script runs the complete Silver pipeline (clean -> components -> index) for a date range
# in a single execution instead of monthly loops, reducing YARN startup overhead.
#
# Options:
#   --start TIMESTAMP    ISO timestamp (UTC) for start of window (required)
#   --end TIMESTAMP      ISO timestamp (UTC) for end of window (required)  
#   --mode MODE          Processing mode: merge (default) or replace
#   --location-id ID     Limit to specific location ID (can be repeated)
#   --clean-only         Run only the clean step
#   --components-only    Run only the components step
#   --index-only         Run only the index step
#   --chunk-months N     Split ranges longer than N months into chunks (default: 6)
#   --no-chunking        Disable automatic chunking for large ranges
#   --dry-run            Show what would be executed without running jobs
#   --help               Show this help

# Parse arguments
START_TS=""
END_TS=""
MODE="merge"
LOCATION_IDS=()
CLEAN_ONLY=false
COMPONENTS_ONLY=false
INDEX_ONLY=false
CHUNK_MONTHS=6
NO_CHUNKING=false
DRY_RUN=false

show_usage() {
  head -n 20 "$0" | tail -n +3 | sed 's/^# //'
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --start)
      START_TS="$2"
      shift 2
      ;;
    --end)
      END_TS="$2"
      shift 2
      ;;
    --mode)
      MODE="$2"
      shift 2
      ;;
    --location-id)
      LOCATION_IDS+=("$2")
      shift 2
      ;;
    --clean-only)
      CLEAN_ONLY=true
      shift
      ;;
    --components-only)
      COMPONENTS_ONLY=true
      shift
      ;;
    --index-only)
      INDEX_ONLY=true
      shift
      ;;
    --chunk-months)
      CHUNK_MONTHS="$2"
      shift 2
      ;;
    --no-chunking)
      NO_CHUNKING=true
      shift
      ;;
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    --help|-h)
      show_usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      echo "Use --help for usage information" >&2
      exit 1
      ;;
  esac
done

if [[ -z "$START_TS" || -z "$END_TS" ]]; then
  echo "Error: --start and --end are required" >&2
  echo "Use --help for usage information" >&2
  exit 1
fi

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

# Function to calculate months between two dates
months_between() {
  local start_date="$1"
  local end_date="$2"
  
  # Extract year-month from ISO timestamps
  start_ym=$(echo "$start_date" | sed 's/T.*//' | cut -d- -f1,2)
  end_ym=$(echo "$end_date" | sed 's/T.*//' | cut -d- -f1,2)
  
  start_year=$(echo "$start_ym" | cut -d- -f1)
  start_month=$(echo "$start_ym" | cut -d- -f2)
  end_year=$(echo "$end_ym" | cut -d- -f1)
  end_month=$(echo "$end_ym" | cut -d- -f2)

  # Use 10# to force decimal interpretation (avoid leading-zero octal parsing like 09)
  echo $(( (10#$end_year - 10#$start_year) * 12 + (10#$end_month - 10#$start_month) ))
}

# Function to add months to a date
add_months() {
  local date="$1"
  local months="$2"
  
  # Use Python to handle date arithmetic properly
  python3 -c "
from datetime import datetime
import calendar
date = datetime.fromisoformat('${date}'.replace('Z', '+00:00'))
year = date.year
month = date.month
day = date.day
hour = date.hour
minute = date.minute
second = date.second

month += $months
while month > 12:
    month -= 12
    year += 1
while month < 1:
    month += 12  
    year -= 1

# Handle end of month edge cases
max_day = calendar.monthrange(year, month)[1]
if day > max_day:
    day = max_day

result = datetime(year, month, day, hour, minute, second)
print(result.isoformat())
"
}

# Function to process a single chunk
process_chunk() {
  local chunk_start="$1"
  local chunk_end="$2"
  local chunk_num="$3"
  local total_chunks="$4"
  
  echo "=> Processing chunk $chunk_num/$total_chunks: $chunk_start -> $chunk_end"
  
  # Build chunk-specific args
  local chunk_args=(--start "$chunk_start" --end "$chunk_end" --mode "$MODE")
  for loc_id in "${LOCATION_IDS[@]}"; do
    chunk_args+=(--location-id "$loc_id")
  done

  # Execute pipeline steps for this chunk
  if [[ "$RUN_CLEAN" == true ]]; then
    echo "=> Chunk $chunk_num/$total_chunks: Clean hourly data"
    if [[ "$DRY_RUN" == true ]]; then
      echo "   [DRY-RUN] Would run: bash scripts/submit_yarn.sh silver/clean_hourly.py ${chunk_args[*]}"
    else
      bash "$ROOT_DIR/scripts/submit_yarn.sh" silver/clean_hourly.py "${chunk_args[@]}"
    fi
  fi

  if [[ "$RUN_COMPONENTS" == true ]]; then
    echo "=> Chunk $chunk_num/$total_chunks: Compute components"
    if [[ "$DRY_RUN" == true ]]; then
      echo "   [DRY-RUN] Would run: bash scripts/submit_yarn.sh silver/components_hourly.py ${chunk_args[*]}"
    else
      bash "$ROOT_DIR/scripts/submit_yarn.sh" silver/components_hourly.py "${chunk_args[@]}"
    fi
  fi

  if [[ "$RUN_INDEX" == true ]]; then
    echo "=> Chunk $chunk_num/$total_chunks: Compute AQI index"
    if [[ "$DRY_RUN" == true ]]; then
      echo "   [DRY-RUN] Would run: bash scripts/submit_yarn.sh silver/index_hourly.py ${chunk_args[*]}"
    else
      bash "$ROOT_DIR/scripts/submit_yarn.sh" silver/index_hourly.py "${chunk_args[@]}"
    fi
  fi
  
  echo "=> Chunk $chunk_num/$total_chunks completed"
}

# Build common args
COMMON_ARGS=(--start "$START_TS" --end "$END_TS" --mode "$MODE")
for loc_id in "${LOCATION_IDS[@]}"; do
  COMMON_ARGS+=(--location-id "$loc_id")
done

echo "=> Running Silver pipeline for $START_TS -> $END_TS (mode: $MODE)"

# Determine which steps to run (make available before chunking)
RUN_CLEAN=true
RUN_COMPONENTS=true
RUN_INDEX=true

if [[ "$CLEAN_ONLY" == true ]]; then
  RUN_COMPONENTS=false
  RUN_INDEX=false
elif [[ "$COMPONENTS_ONLY" == true ]]; then
  RUN_CLEAN=false
  RUN_INDEX=false
elif [[ "$INDEX_ONLY" == true ]]; then
  RUN_CLEAN=false
  RUN_COMPONENTS=false
fi

# Check if we should chunk the processing
MONTHS_DIFF=0
if [[ "$NO_CHUNKING" != true ]]; then
  MONTHS_DIFF=$(months_between "$START_TS" "$END_TS")
  echo "=> Date range spans $MONTHS_DIFF months"
  
  if (( MONTHS_DIFF > CHUNK_MONTHS )); then
    echo "=> Large range detected (>${CHUNK_MONTHS} months), splitting into chunks..."
    
    # Calculate number of chunks needed
    TOTAL_CHUNKS=$(( (MONTHS_DIFF + CHUNK_MONTHS - 1) / CHUNK_MONTHS ))
    echo "=> Will process $TOTAL_CHUNKS chunks of ~$CHUNK_MONTHS months each"
    
    # Process each chunk
    current_start="$START_TS"
    echo "=> Chunk boundaries:"
    for ((i=1; i<=TOTAL_CHUNKS; i++)); do
      if [[ $i -eq $TOTAL_CHUNKS ]]; then
        # Last chunk: use original end timestamp
        chunk_end="$END_TS"
      else
        # Calculate chunk end (start of next chunk)
        chunk_end=$(add_months "$current_start" "$CHUNK_MONTHS")
      fi
      echo "   Chunk $i/$TOTAL_CHUNKS: $current_start -> $chunk_end"
      current_start="$chunk_end"
    done
    echo
    
    # Process each chunk
    current_start="$START_TS"
    for ((i=1; i<=TOTAL_CHUNKS; i++)); do
      if [[ $i -eq $TOTAL_CHUNKS ]]; then
        # Last chunk: use original end timestamp
        chunk_end="$END_TS"
      else
        # Calculate chunk end (start of next chunk)
        chunk_end=$(add_months "$current_start" "$CHUNK_MONTHS")
      fi
      
      echo "=> Starting chunk $i/$TOTAL_CHUNKS processing..."
      process_chunk "$current_start" "$chunk_end" "$i" "$TOTAL_CHUNKS"
      echo "=> Finished chunk $i/$TOTAL_CHUNKS"
      echo
      
      # Set start for next chunk
      if [[ $i -lt $TOTAL_CHUNKS ]]; then
        current_start="$chunk_end"
      fi
    done
    
    echo "=> All chunks completed successfully"
    exit 0
  else
    echo "=> Range is manageable (≤${CHUNK_MONTHS} months), processing in single batch"
  fi
fi
# Execute pipeline steps
if [[ "$RUN_CLEAN" == true ]]; then
  echo "=> Step 1: Clean hourly data"
  if [[ "$DRY_RUN" == true ]]; then
    echo "   [DRY-RUN] Would run: bash scripts/submit_yarn.sh silver/clean_hourly.py ${COMMON_ARGS[*]}"
  else
    bash "$ROOT_DIR/scripts/submit_yarn.sh" silver/clean_hourly.py "${COMMON_ARGS[@]}"
  fi
  echo "=> Clean step completed"
fi

if [[ "$RUN_COMPONENTS" == true ]]; then
  echo "=> Step 2: Compute components"
  if [[ "$DRY_RUN" == true ]]; then
    echo "   [DRY-RUN] Would run: bash scripts/submit_yarn.sh silver/components_hourly.py ${COMMON_ARGS[*]}"
  else
    bash "$ROOT_DIR/scripts/submit_yarn.sh" silver/components_hourly.py "${COMMON_ARGS[@]}"
  fi
  echo "=> Components step completed"
fi

if [[ "$RUN_INDEX" == true ]]; then
  echo "=> Step 3: Compute AQI index"
  if [[ "$DRY_RUN" == true ]]; then
    echo "   [DRY-RUN] Would run: bash scripts/submit_yarn.sh silver/index_hourly.py ${COMMON_ARGS[*]}"
  else
    bash "$ROOT_DIR/scripts/submit_yarn.sh" silver/index_hourly.py "${COMMON_ARGS[@]}"
  fi
  echo "=> Index step completed"
fi

echo "=> Silver pipeline completed successfully"