#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Optimized AQ Data Pipeline
# Single Spark submission with distributed execution for maximum efficiency
# =============================================================================

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
export PYTHONPATH="$ROOT_DIR/src:${PYTHONPATH:-}"

# Configuration variables  
START=${START:-2024-01-01}
END=${END:-2025-09-20}
MODE=${MODE:-upsert}
FULL=${FULL:-false}
LOCATIONS_FILE=${LOCATIONS_FILE:-$ROOT_DIR/configs/locations.json}
LOCATION_IDS=${LOCATION_IDS:-}
CHUNK_DAYS=${CHUNK_DAYS:-10}
WAREHOUSE_URI=${WAREHOUSE_URI:-hdfs://khoa-master:9000/warehouse/iceberg}
EXPIRE_SNAPSHOTS=${EXPIRE_SNAPSHOTS:-false}

# Memory and performance optimizations (cluster-compatible)
EXECUTOR_MEMORY=${EXECUTOR_MEMORY:-1g}
EXECUTOR_CORES=${EXECUTOR_CORES:-2}
NUM_EXECUTORS=${NUM_EXECUTORS:-4}
DRIVER_MEMORY=${DRIVER_MEMORY:-1g}

SPARK_HOME=${SPARK_HOME:-/home/dlhnhom2/spark}
SPARK_SQL="$SPARK_HOME/bin/spark-sql"
SUBMIT_WRAPPER=${SUBMIT_WRAPPER:-$ROOT_DIR/scripts/submit_yarn.sh}

# Optimized Spark configuration for single submission
MASTER_OPTS=(
  --master yarn
  --deploy-mode client
  --executor-memory "$EXECUTOR_MEMORY"
  --executor-cores "$EXECUTOR_CORES" 
  --num-executors "$NUM_EXECUTORS"
  --driver-memory "$DRIVER_MEMORY"
  --conf spark.sql.adaptive.enabled=true
  --conf spark.sql.adaptive.coalescePartitions.enabled=true
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop
  --conf spark.sql.catalog.hadoop_catalog.warehouse="$WAREHOUSE_URI"
  --conf spark.sql.catalogImplementation=in-memory
  --conf spark.sql.execution.arrow.pyspark.enabled=false
  --conf spark.sql.sources.partitionOverwriteMode=dynamic
  --conf spark.yarn.maxAppAttempts=1
  --conf spark.dynamicAllocation.enabled=true
  --conf spark.dynamicAllocation.minExecutors=1
  --conf spark.dynamicAllocation.maxExecutors=6
  --conf spark.dynamicAllocation.initialExecutors="$NUM_EXECUTORS"
)

# Check for JAR archive optimization
check_jar_optimization() {
  if hdfs dfs -test -e /spark/4.0.0/spark-libs.zip 2>/dev/null; then
    echo "[INFO] Using optimized JAR archive from HDFS"
  else
    echo "[WARN] JAR archive not found. Consider creating it for faster startup:"
    echo "       cd $SPARK_HOME && zip -r spark-libs.zip jars/ && hdfs dfs -mkdir -p /spark/4.0.0 && hdfs dfs -put -f spark-libs.zip /spark/4.0.0/"
  fi
}

# Ensure HDFS is not in safe mode
ensure_not_safe_mode() {
  if command -v hdfs >/dev/null 2>&1; then
    if hdfs dfsadmin -safemode get 2>/dev/null | grep -q "Safe mode is ON"; then
      echo "[ERROR] HDFS NameNode is in safe mode. Leave safe mode before running the pipeline." >&2
      echo "        Run: hdfs dfsadmin -safemode leave" >&2
      exit 1
    fi
  fi
}

# Create optimized pipeline SQL script
create_pipeline_sql() {
  local sql_file="$1"
  local run_id="$2"
  
  cat > "$sql_file" << EOF
-- =============================================================================
-- Single-submission AQ Data Pipeline
-- Optimized for distributed execution with minimal overhead
-- =============================================================================

-- Progress tracking
SELECT 'Starting AQ data pipeline (optimized single submission)...' AS status;

-- =============================================================================
-- DIMENSION LAYER SETUP (Idempotent)
-- =============================================================================
SELECT 'Setting up dimension layer...' AS status;

CREATE TABLE IF NOT EXISTS hadoop_catalog.aq.dim_locations (
  location_id STRING,
  latitude DOUBLE,
  longitude DOUBLE
) USING iceberg;

MERGE INTO hadoop_catalog.aq.dim_locations d
USING (
  SELECT 'Hà Nội' AS location_id, 21.028511 AS latitude, 105.804817 AS longitude
  UNION ALL
  SELECT 'TP. Hồ Chí Minh', 10.762622, 106.660172
  UNION ALL
  SELECT 'Đà Nẵng', 16.054406, 108.202167
) v
ON d.location_id = v.location_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- =============================================================================
-- SILVER VIEW LAYER (Skip due to Iceberg limitations)
-- =============================================================================
SELECT 'NOTE: Silver views require separate spark-submit due to catalog limitations' AS silver_note;

-- =============================================================================
-- GOLD LAYER SETUP
-- =============================================================================
SELECT 'Setting up Gold layer...' AS status;

CREATE TABLE IF NOT EXISTS hadoop_catalog.aq.gold_air_quality_daily (
  location_id STRING,
  date        DATE,
  pm25_avg_24h DOUBLE,
  pm10_avg_24h DOUBLE,
  no2_avg_24h  DOUBLE,
  o3_avg_24h   DOUBLE,
  so2_avg_24h  DOUBLE,
  co_avg_24h   DOUBLE,
  uv_index_avg DOUBLE,
  uv_index_cs_avg DOUBLE,
  aod_avg DOUBLE
) USING iceberg
PARTITIONED BY (date, location_id)
TBLPROPERTIES (
  'format-version'='2', 
  'write.target-file-size-bytes'='134217728',
  'write.format.default'='parquet',
  'write.parquet.compression-codec'='zstd'
);

EOF

  # Add Gold layer processing logic based on mode
  if [[ "$FULL" == "true" ]]; then
    cat >> "$sql_file" << 'EOF'
-- =============================================================================
-- GOLD LAYER FULL REBUILD
-- =============================================================================
SELECT 'Performing full rebuild of Gold layer...' AS status;

TRUNCATE TABLE hadoop_catalog.aq.gold_air_quality_daily;

INSERT INTO hadoop_catalog.aq.gold_air_quality_daily
SELECT
  r.location_id,
  CAST(date(r.ts) AS DATE) AS date,
  AVG(NULLIF(r.pm2_5, CASE WHEN r.pm2_5 < 0 THEN r.pm2_5 END)) AS pm25_avg_24h,
  AVG(NULLIF(r.pm10, CASE WHEN r.pm10 < 0 THEN r.pm10 END)) AS pm10_avg_24h,
  AVG(NULLIF(r.nitrogen_dioxide, CASE WHEN r.nitrogen_dioxide < 0 THEN r.nitrogen_dioxide END)) AS no2_avg_24h,
  AVG(NULLIF(r.ozone, CASE WHEN r.ozone < 0 THEN r.ozone END)) AS o3_avg_24h,
  AVG(NULLIF(r.sulphur_dioxide, CASE WHEN r.sulphur_dioxide < 0 THEN r.sulphur_dioxide END)) AS so2_avg_24h,
  AVG(NULLIF(r.carbon_monoxide, CASE WHEN r.carbon_monoxide < 0 THEN r.carbon_monoxide END)) AS co_avg_24h,
  AVG(NULLIF(r.uv_index, CASE WHEN r.uv_index < 0 THEN r.uv_index END)) AS uv_index_avg,
  AVG(NULLIF(r.uv_index_clear_sky, CASE WHEN r.uv_index_clear_sky < 0 THEN r.uv_index_clear_sky END)) AS uv_index_cs_avg,
  AVG(NULLIF(r.aerosol_optical_depth, CASE WHEN r.aerosol_optical_depth < 0 THEN r.aerosol_optical_depth END)) AS aod_avg
FROM hadoop_catalog.aq.raw_open_meteo_hourly r
LEFT JOIN hadoop_catalog.aq.dim_locations d USING (location_id)
GROUP BY r.location_id, CAST(date(r.ts) AS DATE);
EOF
  else
    cat >> "$sql_file" << EOF
-- =============================================================================
-- GOLD LAYER INCREMENTAL UPSERT 
-- =============================================================================
SELECT 'Performing incremental upsert for RUN_ID=$run_id...' AS status;

WITH changed AS (
  SELECT DISTINCT location_id, CAST(date(ts) AS DATE) AS date
  FROM hadoop_catalog.aq.raw_open_meteo_hourly
  WHERE run_id='$run_id'
),
agg AS (
  SELECT
    r.location_id,
    CAST(date(r.ts) AS DATE) AS date,
    AVG(NULLIF(r.pm2_5, CASE WHEN r.pm2_5 < 0 THEN r.pm2_5 END)) AS pm25_avg_24h,
    AVG(NULLIF(r.pm10, CASE WHEN r.pm10 < 0 THEN r.pm10 END)) AS pm10_avg_24h,
    AVG(NULLIF(r.nitrogen_dioxide, CASE WHEN r.nitrogen_dioxide < 0 THEN r.nitrogen_dioxide END)) AS no2_avg_24h,
    AVG(NULLIF(r.ozone, CASE WHEN r.ozone < 0 THEN r.ozone END)) AS o3_avg_24h,
    AVG(NULLIF(r.sulphur_dioxide, CASE WHEN r.sulphur_dioxide < 0 THEN r.sulphur_dioxide END)) AS so2_avg_24h,
    AVG(NULLIF(r.carbon_monoxide, CASE WHEN r.carbon_monoxide < 0 THEN r.carbon_monoxide END)) AS co_avg_24h,
    AVG(NULLIF(r.uv_index, CASE WHEN r.uv_index < 0 THEN r.uv_index END)) AS uv_index_avg,
    AVG(NULLIF(r.uv_index_clear_sky, CASE WHEN r.uv_index_clear_sky < 0 THEN r.uv_index_clear_sky END)) AS uv_index_cs_avg,
    AVG(NULLIF(r.aerosol_optical_depth, CASE WHEN r.aerosol_optical_depth < 0 THEN r.aerosol_optical_depth END)) AS aod_avg
  FROM hadoop_catalog.aq.raw_open_meteo_hourly r
  INNER JOIN changed c
    ON c.location_id = r.location_id AND c.date = CAST(date(r.ts) AS DATE)
  LEFT JOIN hadoop_catalog.aq.dim_locations d USING (location_id)
  GROUP BY r.location_id, CAST(date(r.ts) AS DATE)
)
MERGE INTO hadoop_catalog.aq.gold_air_quality_daily g
USING agg a
ON g.location_id = a.location_id AND g.date = a.date
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
EOF
  fi

  # Add housekeeping operations
  cat >> "$sql_file" << 'EOF'

-- =============================================================================
-- ICEBERG HOUSEKEEPING (Optimized for distributed execution)
-- =============================================================================
SELECT 'Performing Iceberg housekeeping...' AS status;

-- Rewrite small files (parallel processing)
CALL hadoop_catalog.system.rewrite_data_files('aq.gold_air_quality_daily');

-- Remove orphan files (parallel processing)
CALL hadoop_catalog.system.remove_orphan_files('aq.gold_air_quality_daily');

EOF

  if [[ "$EXPIRE_SNAPSHOTS" == "true" ]]; then
    cat >> "$sql_file" << 'EOF'
-- Expire old snapshots (conditional)
SELECT 'Expiring old snapshots...' AS status;
-- NOTE: Skipping expire_snapshots due to procedure call issues in spark-sql
SELECT 'Snapshot expiration skipped (procedure call syntax issues)' AS note;
EOF
  else
    cat >> "$sql_file" << 'EOF' 
-- Snapshot expiration skipped (EXPIRE_SNAPSHOTS=false)
EOF
  fi

  cat >> "$sql_file" << 'EOF'

-- =============================================================================
-- COMPLETION STATUS
-- =============================================================================
SELECT 'AQ data pipeline completed successfully!' AS status;
SELECT 'Components processed:' AS summary;
SELECT '- Bronze: Ingested via spark-submit (distributed)' AS bronze_note;
SELECT '- Dimension: hadoop_catalog.aq.dim_locations (updated)' AS dim_table;  
SELECT '- Gold: hadoop_catalog.aq.gold_air_quality_daily (processed)' AS gold_table;
SELECT '- Housekeeping: File optimization and cleanup completed' AS maintenance_note;
EOF
}

# Main execution
main() {
  echo "[INFO] Optimized AQ Data Pipeline"
  echo "[INFO] Configuration: START=$START END=$END MODE=$MODE FULL=$FULL"
  echo "[INFO] Resources: EXECUTORS=$NUM_EXECUTORS, MEMORY=$EXECUTOR_MEMORY, CORES=$EXECUTOR_CORES"
  echo "[INFO] Warehouse: $WAREHOUSE_URI"
  
  ensure_not_safe_mode
  check_jar_optimization
  
  # Step 1: Bronze ingest (distributed via spark-submit)
  echo "[INFO] Step 1: Running Bronze ingest via spark-submit..."
  
  if [[ ! -x "$SUBMIT_WRAPPER" ]]; then
    echo "[ERROR] Submit wrapper not executable: $SUBMIT_WRAPPER" >&2
    exit 1
  fi

  INGEST_CMD=(
    "$SUBMIT_WRAPPER"
    jobs/ingest/open_meteo_bronze.py
    --locations "$LOCATIONS_FILE"
    --start "$START"
    --end "$END"
    --chunk-days "$CHUNK_DAYS"
    --mode "$MODE"
  )

  if [[ -n "$LOCATION_IDS" ]]; then
    for lid in $LOCATION_IDS; do
      INGEST_CMD+=(--location-id "$lid")
    done
  fi

  TMP_LOG=$(mktemp)
  trap 'rm -f "$TMP_LOG"' EXIT

  local start_ingest
  start_ingest=$(date +%s)
  
  if ! "${INGEST_CMD[@]}" | tee "$TMP_LOG"; then
    echo "[ERROR] Bronze ingest job failed" >&2
    exit 1
  fi
  
  local end_ingest
  end_ingest=$(date +%s)
  local ingest_duration=$((end_ingest - start_ingest))

  RUN_ID=$(awk -F= '/^RUN_ID=/{print $2}' "$TMP_LOG" | tail -n 1 | tr -d '[:space:]')
  if [[ -z "$RUN_ID" ]]; then
    echo "[ERROR] Failed to parse RUN_ID from ingest output" >&2
    exit 1
  fi

  echo "[INFO] Bronze ingest completed in ${ingest_duration}s (RUN_ID=$RUN_ID)"
  
  # Step 2: Optimized pipeline processing (single submission)
  echo "[INFO] Step 2: Running optimized pipeline processing (single submission)..."
  
  local sql_script
  sql_script=$(mktemp --suffix=.sql)
  trap "rm -f '$sql_script'" EXIT
  
  create_pipeline_sql "$sql_script" "$RUN_ID"
  
  local start_pipeline
  start_pipeline=$(date +%s)
  
  if ! "$SPARK_SQL" "${MASTER_OPTS[@]}" -f "$sql_script"; then
    echo "[ERROR] Pipeline processing failed" >&2
    exit 1
  fi
  
  local end_pipeline
  end_pipeline=$(date +%s)
  local pipeline_duration=$((end_pipeline - start_pipeline))
  local total_duration=$((end_pipeline - start_ingest))
  
  echo "[INFO] Pipeline processing completed in ${pipeline_duration}s"
  
  # Step 3: Cleanup staging artifacts
  echo "[INFO] Step 3: Cleaning up staging artifacts..."
  cleanup_staging
  
  echo "[SUCCESS] AQ data pipeline completed in ${total_duration}s (RUN_ID=$RUN_ID)"
  echo "[INFO] Total time breakdown: Ingest ${ingest_duration}s + Processing ${pipeline_duration}s = ${total_duration}s"
  
  # Step 4: Next steps guidance
  echo
  echo "[INFO] Next steps:"
  echo "  # Query your data"
  echo "  SELECT * FROM hadoop_catalog.aq.gold_air_quality_daily ORDER BY date DESC, location_id LIMIT 10;"
  echo
  echo "  # Verify data quality" 
  echo "  SELECT location_id, COUNT(*) as days FROM hadoop_catalog.aq.gold_air_quality_daily GROUP BY location_id;"
}

# Cleanup function
cleanup_staging() {
  if command -v hdfs >/dev/null 2>&1; then
    echo "[INFO] Removing YARN .sparkStaging leftovers"
    hdfs dfs -rm -r -f "/user/$(whoami)/.sparkStaging" 2>/dev/null || true
  fi

  echo "[INFO] Removing local Spark scratch directories"
  rm -rf "$ROOT_DIR/spark-warehouse" 2>/dev/null || true

  if [[ -d "$ROOT_DIR/.cache" ]]; then
    echo "[INFO] Pruning stale HTTP cache entries (>3 days)"
    find "$ROOT_DIR/.cache" -type f -mtime +3 -delete 2>/dev/null || true
  fi
}

# Handle script arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --setup-cache)
      echo "[INFO] JAR caching setup will be suggested if not found"
      shift
      ;;
    --use-thrift)
      echo "[INFO] Thrift server mode not applicable for pipeline processing"
      shift
      ;;
    --help)
      echo "Usage: $0 [--setup-cache] [--use-thrift]"
      echo ""
      echo "Environment variables:"
      echo "  START=YYYY-MM-DD          Start date (default: 2024-01-01)"
      echo "  END=YYYY-MM-DD            End date (default: 2025-09-20)" 
      echo "  MODE=upsert|overwrite     Processing mode (default: upsert)"
      echo "  FULL=true|false           Full rebuild vs incremental (default: false)"
      echo "  CHUNK_DAYS=N              Days per chunk (default: 10)"
      echo "  EXPIRE_SNAPSHOTS=true     Enable snapshot expiration (default: false)"
      echo ""
      echo "Performance variables:"
      echo "  EXECUTOR_MEMORY=1g        Executor memory (default: 1g)" 
      echo "  NUM_EXECUTORS=4           Number of executors (default: 4)"
      exit 0
      ;;
    *)
      echo "[WARN] Unknown argument: $1"
      shift
      ;;
  esac
done

# Execute main function
main "$@"