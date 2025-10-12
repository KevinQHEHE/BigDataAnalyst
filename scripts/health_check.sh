#!/usr/bin/env bash

# Air Quality Lakehouse quick health check
# This script runs lightweight diagnostics against key services that power the
# data lakehouse stack (Hadoop, Spark, Iceberg, Superset, MLflow, Prefect, etc.).
# It prints OK/WARN/ERROR lines for each probe instead of exiting on the first failure.

set -u
set -o pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ICEBERG_JAR_DEFAULT="/home/dlhnhom2/spark/jars/iceberg-spark-runtime-4.0_2.13-1.10.0.jar"
ICEBERG_JAR="${ICEBERG_JAR:-$ICEBERG_JAR_DEFAULT}"
HADOOP_WAREHOUSE="${HADOOP_WAREHOUSE:-hdfs:///warehouse/iceberg_test}"
THRIFT_HOST="${THRIFT_HOST:-localhost}"
THRIFT_PORT="${THRIFT_PORT:-10000}"
ICEBERG_NAMESPACE="${ICEBERG_NAMESPACE:-default}"
ICEBERG_TABLE="${ICEBERG_TABLE:-air_quality}"

TOTAL_WARNINGS=0
TOTAL_ERRORS=0

log_section() {
  printf '\n== %s ==\n' "$1"
}

indent_output() {
  sed 's/^/    /'
}

log_status() {
  local level="$1"
  shift
  local message="$*"
  printf '[%s] %s\n' "$level" "$message"
  case "$level" in
    WARN) TOTAL_WARNINGS=$((TOTAL_WARNINGS + 1)) ;;
    ERROR) TOTAL_ERRORS=$((TOTAL_ERRORS + 1)) ;;
  esac
}

run_check() {
  local label="$1"
  local cmd="$2"
  printf '%s %s\n' '--' "$label"

  local output
  if output=$(eval "$cmd" 2>&1); then
    log_status OK "$label"
    if [[ -n "$output" ]]; then
      printf '%s\n' "$output" | indent_output
    fi
  else
    local exit_code=$?
    log_status WARN "$label (exit code $exit_code)"
    if [[ -n "$output" ]]; then
      printf '%s\n' "$output" | indent_output
    fi
  fi
}

command_status() {
  local cmd="$1"
  if command -v "$cmd" >/dev/null 2>&1; then
    log_status OK "$cmd located at $(command -v "$cmd")"
    return 0
  else
    log_status ERROR "$cmd not found in PATH"
    return 1
  fi
}

log_section "Environment"
run_check "Operating system details" "cat /etc/os-release"
run_check "Kernel" "uname -a"
run_check "Hostname" "hostnamectl 2>/dev/null || hostname"

log_section "Languages & Libraries"
if command_status python3; then
  run_check "Python version" "python3 --version"
  run_check "PySpark import" "python3 -c 'import pyspark; print(pyspark.__version__)'"
  run_check "MLflow import" "python3 -c \"import mlflow; import pkg_resources; print('MLflow:', mlflow.__version__); print('SQLAlchemy:', pkg_resources.get_distribution('SQLAlchemy').version)\""
fi

if command_status python3; then
  run_check "pip environment snapshot" "python3 -m pip list | grep -E 'pyspark|pyarrow|mlflow|apache-superset|sqlalchemy'"
fi

log_section "Hadoop"
if command_status hdfs; then
  run_check "HDFS report (head)" "hdfs dfsadmin -report | head -n 20"
  run_check "Warehouse path" "hdfs dfs -ls $HADOOP_WAREHOUSE"
  run_check "Catalog namespace $ICEBERG_NAMESPACE" "hdfs dfs -ls $HADOOP_WAREHOUSE/$ICEBERG_NAMESPACE"
  run_check "Iceberg metadata dir ($ICEBERG_NAMESPACE.$ICEBERG_TABLE)" "hdfs dfs -ls $HADOOP_WAREHOUSE/$ICEBERG_NAMESPACE/$ICEBERG_TABLE/metadata | head -n 20"
fi

log_section "Spark & Iceberg"
if command_status spark-submit; then
  run_check "spark-submit version" "spark-submit --version"
fi

if [[ -f "$ICEBERG_JAR" ]]; then
  log_status OK "Iceberg runtime jar detected at $ICEBERG_JAR"
else
  log_status WARN "Iceberg runtime jar not found at $ICEBERG_JAR"
fi

if command_status spark-sql; then
  if [[ -f "$ICEBERG_JAR" ]]; then
    run_check "Iceberg catalog tables" "spark-sql \
      --master local[2] \
      --conf spark.sql.catalogImplementation=in-memory \
      --conf spark.jars=$ICEBERG_JAR \
      --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
      --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog \
      --conf spark.sql.catalog.hadoop_catalog.type=hadoop \
      --conf spark.sql.catalog.hadoop_catalog.warehouse=$HADOOP_WAREHOUSE \
      -e 'SHOW TABLES IN hadoop_catalog.default'"
  else
    log_status WARN "Skipping Iceberg catalog probe because runtime jar is missing"
  fi
fi

log_section "Orchestration & Prefect"
if command_status prefect; then
  run_check "Prefect version" "prefect version"
  run_check "Prefect config" "prefect config view --show-defaults | head -n 20"
fi

log_section "Service Connectivity"
if command_status ss; then
  run_check "Spark Thrift Server port scan" "ss -ltnp | grep $THRIFT_PORT"
elif command_status nc; then
  run_check "Spark Thrift Server port scan" "nc -zv $THRIFT_HOST $THRIFT_PORT"
else
  log_status WARN "Neither ss nor nc available to test Spark Thrift Server port"
fi

if command_status superset; then
  run_check "Superset version" "superset version"
else
  log_status WARN "Superset CLI not available"
fi

if command_status mlflow; then
  run_check "MLflow version" "mlflow --version"
fi

printf '\nSummary: %d warning(s), %d error(s)\n' "$TOTAL_WARNINGS" "$TOTAL_ERRORS"
if (( TOTAL_ERRORS > 0 )); then
  exit 2
elif (( TOTAL_WARNINGS > 0 )); then
  exit 1
fi
