#!/usr/bin/env bash
set -euo pipefail

# =============================================================================
# Optimized AQ Lakehouse Reset Pipeline
# Single Spark submission with distributed execution for maximum efficiency
# =============================================================================

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
WAREHOUSE_URI=${WAREHOUSE_URI:-hdfs://khoa-master:9000/warehouse/iceberg}
RESET_EXPIRE_SNAPSHOTS=${RESET_EXPIRE_SNAPSHOTS:-true}

# Memory and performance optimizations
EXECUTOR_MEMORY=${EXECUTOR_MEMORY:-1g}
EXECUTOR_CORES=${EXECUTOR_CORES:-2}
NUM_EXECUTORS=${NUM_EXECUTORS:-4}
DRIVER_MEMORY=${DRIVER_MEMORY:-1g}

SPARK_HOME=${SPARK_HOME:-/home/dlhnhom2/spark}
SPARK_SQL="$SPARK_HOME/bin/spark-sql"

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
    MASTER_OPTS+=(--conf spark.yarn.archive=hdfs:///spark/4.0.0/spark-libs.zip)
  else
    echo "[WARN] JAR archive not found. Consider creating it for faster startup:"
    echo "       cd $SPARK_HOME && zip -r spark-libs.zip jars/ && hdfs dfs -mkdir -p /spark/4.0.0 && hdfs dfs -put -f spark-libs.zip /spark/4.0.0/"
  fi
}

ensure_not_safe_mode() {
  if ! command -v hdfs >/dev/null 2>&1; then
    return 0
  fi

  local status
  if ! status=$(hdfs dfsadmin -safemode get 2>/dev/null); then
    echo "[WARN] Unable to query HDFS safe mode status" >&2
    return 0
  fi

  if echo "$status" | grep -qi "ON"; then
    echo "[ERROR] HDFS NameNode is in safe mode. Leave safe mode before running the reset." >&2
    echo "        Run: hdfs dfsadmin -safemode leave" >&2
    exit 1
  fi
}

# Create optimized reset SQL script
create_reset_sql() {
  local sql_file="$1"
  cat > "$sql_file" << 'EOF'
-- =============================================================================
-- Single-submission AQ Lakehouse Reset & Prepare
-- Optimized for distributed execution with minimal overhead
-- =============================================================================

-- Progress tracking
SELECT 'Starting AQ lakehouse reset (optimized single submission)...' AS status;

-- Create namespaces (idempotent)
CREATE NAMESPACE IF NOT EXISTS hadoop_catalog.aq;
CREATE NAMESPACE IF NOT EXISTS spark_catalog.aq;

-- =============================================================================
-- BRONZE LAYER RESET
-- =============================================================================
SELECT 'Resetting Bronze layer...' AS status;

CREATE TABLE IF NOT EXISTS hadoop_catalog.aq.raw_open_meteo_hourly (
  location_id STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  ts TIMESTAMP,
  aerosol_optical_depth DOUBLE,
  pm2_5 DOUBLE,
  pm10 DOUBLE,
  dust DOUBLE,
  nitrogen_dioxide DOUBLE,
  ozone DOUBLE,
  sulphur_dioxide DOUBLE,
  carbon_monoxide DOUBLE,
  uv_index DOUBLE,
  uv_index_clear_sky DOUBLE,
  source STRING,
  run_id STRING,
  ingested_at TIMESTAMP
) USING iceberg
PARTITIONED BY (days(ts))
TBLPROPERTIES (
  'format-version'='2',
  'write.target-file-size-bytes'='134217728',
  'write.format.default'='parquet',
  'write.parquet.compression-codec'='zstd'
);

TRUNCATE TABLE hadoop_catalog.aq.raw_open_meteo_hourly;

-- =============================================================================
-- DIMENSION LAYER RESET  
-- =============================================================================
SELECT 'Resetting dimension layer...' AS status;

CREATE TABLE IF NOT EXISTS hadoop_catalog.aq.dim_locations (
  location_id STRING,
  latitude DOUBLE,
  longitude DOUBLE,
  city STRING,
  country STRING,
  is_active BOOLEAN,
  effective_from DATE,
  effective_to DATE
) USING iceberg
TBLPROPERTIES ('format-version'='2');

TRUNCATE TABLE hadoop_catalog.aq.dim_locations;

-- =============================================================================
-- GOLD LAYER RESET
-- =============================================================================
SELECT 'Resetting Gold layer...' AS status;

CREATE TABLE IF NOT EXISTS hadoop_catalog.aq.gold_air_quality_daily (
  location_id STRING,
  date DATE,
  pm25_avg_24h DOUBLE,
  pm10_avg_24h DOUBLE,
  no2_avg_24h DOUBLE,
  o3_avg_24h DOUBLE,
  so2_avg_24h DOUBLE,
  co_avg_24h DOUBLE,
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

TRUNCATE TABLE hadoop_catalog.aq.gold_air_quality_daily;

-- =============================================================================
-- ICEBERG MAINTENANCE (Optimized for distributed execution)
-- =============================================================================
SELECT 'Performing Iceberg maintenance (distributed)...' AS status;

-- Remove orphan files (parallel processing)
CALL hadoop_catalog.system.remove_orphan_files('aq.raw_open_meteo_hourly');
CALL hadoop_catalog.system.remove_orphan_files('aq.dim_locations');
CALL hadoop_catalog.system.remove_orphan_files('aq.gold_air_quality_daily');

-- Expire snapshots if enabled
EOF

  if [[ "$RESET_EXPIRE_SNAPSHOTS" == "true" ]]; then
    cat >> "$sql_file" << 'EOF'
-- Expire old snapshots (conditional)
CALL hadoop_catalog.system.expire_snapshots(
  'aq.raw_open_meteo_hourly',
  CURRENT_TIMESTAMP - INTERVAL 1 HOURS
);

CALL hadoop_catalog.system.expire_snapshots(
  'aq.dim_locations', 
  CURRENT_TIMESTAMP - INTERVAL 1 HOURS
);

CALL hadoop_catalog.system.expire_snapshots(
  'aq.gold_air_quality_daily',
  CURRENT_TIMESTAMP - INTERVAL 1 HOURS
);
EOF
  else
    cat >> "$sql_file" << 'EOF'
-- Snapshot expiration skipped (RESET_EXPIRE_SNAPSHOTS=false)
EOF
  fi

  cat >> "$sql_file" << 'EOF'

-- =============================================================================
-- COMPLETION STATUS
-- =============================================================================
SELECT 'AQ lakehouse reset completed successfully!' AS status;
SELECT 'Tables created/reset:' AS summary;
SELECT '- Bronze: hadoop_catalog.aq.raw_open_meteo_hourly' AS bronze_table;
SELECT '- Dim: hadoop_catalog.aq.dim_locations' AS dim_table;
SELECT '- Gold: hadoop_catalog.aq.gold_air_quality_daily' AS gold_table;
SELECT '- Silver: Views not included (use separate spark-submit)' AS silver_note;
EOF
}

# Main execution
main() {
  echo "[INFO] Optimized AQ Lakehouse Reset Pipeline"
  echo "[INFO] Configuration: EXECUTORS=$NUM_EXECUTORS, MEMORY=$EXECUTOR_MEMORY, CORES=$EXECUTOR_CORES"
  echo "[INFO] Warehouse: $WAREHOUSE_URI"
  
  ensure_not_safe_mode
  check_jar_optimization
  
  # Create temporary SQL script
  local sql_script
  sql_script=$(mktemp --suffix=.sql)
  trap "rm -f '$sql_script'" EXIT
  
  create_reset_sql "$sql_script"
  
  echo "[INFO] Executing single-submission reset (distributed)..."
  local start_time
  start_time=$(date +%s)
  
  if ! "$SPARK_SQL" "${MASTER_OPTS[@]}" -f "$sql_script"; then
    echo "[ERROR] Reset pipeline failed" >&2
    exit 1
  fi
  
  local end_time
  end_time=$(date +%s)
  local duration=$((end_time - start_time))
  
  echo "[INFO] Cleaning up local artifacts..."
  
  # Clean local staging and cache files
  rm -rf "$ROOT_DIR/metastore_db" \
         "$ROOT_DIR/derby.log" \
         "$ROOT_DIR/spark-warehouse" \
         "$ROOT_DIR/.cache.sqlite"
  
  # Clean HTTP cache (keep recent entries)
  if [[ -d "$ROOT_DIR/.cache" ]]; then
    find "$ROOT_DIR/.cache" -type f -mtime +1 -delete 2>/dev/null || true
  fi
  
  # Clean HDFS staging directories
  if command -v hdfs >/dev/null 2>&1; then
    hdfs dfs -rm -r -f "/user/$(whoami)/.sparkStaging" 2>/dev/null || true
  fi
  
  echo "[SUCCESS] AQ lakehouse reset completed in ${duration}s"
  echo "[INFO] Next: Run your ingest pipeline with optimized settings"
  echo ""
  echo "Quick start commands:"
  echo "  # Regular ingest"
  echo "  START=2024-01-01 END=2024-01-31 ./scripts/run_pipeline.sh"
  echo ""
  echo "  # Update from latest"
  echo "  bash script/submit_yarn.sh ingest/open_meteo_bronze.py \\"
  echo "    --locations configs/locations.json --update-from-db --yes --chunk-days 10"
}

main "$@"