#!/bin/bash
# Deploy Prefect flows with schedules

set -e

echo "=================================================="
echo "Deploying Prefect AQI Pipelines with Schedules"
echo "=================================================="

cd "$(dirname "$0")/.."

# Check if Prefect server is running
if ! prefect server --help &>/dev/null; then
    echo "❌ Prefect is not installed or not in PATH"
    exit 1
fi

echo ""
echo "Step 1: Starting Prefect server (if not running)..."
echo "Checking if Prefect server is accessible..."

# Try to ping Prefect API
if curl -s http://localhost:4200/api/health > /dev/null 2>&1; then
    echo "✓ Prefect server is already running"
else
    echo "⚠ Prefect server not detected. Starting in background..."
    nohup prefect server start > logs/prefect-server.log 2>&1 &
    echo "✓ Prefect server starting... (check logs/prefect-server.log)"
    echo "  Waiting 10 seconds for server to be ready..."
    sleep 10
fi

echo ""
echo "Step 2: Deploying flows from prefect.yaml..."
echo ""

# Deploy all flows defined in prefect.yaml
prefect deploy --all

echo ""
echo "=================================================="
echo "✓ Deployment Complete!"
echo "=================================================="
echo ""
echo "Deployed flows:"
echo "  1. hourly-aqi-pipeline     → Runs every hour (cron: 0 * * * *)"
echo "  2. daily-full-pipeline     → Runs daily at 1 AM (cron: 0 1 * * *)"
echo "  3. backfill-pipeline       → Manual trigger only (no schedule)"
echo ""
echo "Next steps:"
echo "  • View deployments:  prefect deployment ls"
echo "  • View schedules:    prefect deployment inspect hourly-aqi-pipeline"
echo "  • Pause schedule:    prefect deployment pause hourly-aqi-pipeline"
echo "  • Resume schedule:   prefect deployment resume hourly-aqi-pipeline"
echo "  • Manual trigger:    prefect deployment run hourly-aqi-pipeline"
echo "  • View runs:         prefect flow-run ls"
echo "  • Prefect UI:        http://localhost:4200"
echo ""
echo "To run with YARN validation:"
echo "  bash scripts/spark_submit.sh Prefect/full_pipeline_flow.py -- --hourly"
echo ""
