#!/bin/bash
# Deploy Prefect Flows with YARN Integration

set -e

echo "=================================================="
echo "Deploying Prefect AQI Pipelines (YARN Mode)"
echo "=================================================="

cd "$(dirname "$0")/.."

# Check if Prefect is installed
if ! command -v prefect &> /dev/null; then
    echo "❌ Prefect is not installed"
    exit 1
fi

echo ""
echo "Step 1: Creating work pool..."
echo ""

# Create work pool if not exists
if ! prefect work-pool ls | grep -q "default"; then
    prefect work-pool create default --type process
    echo "✓ Work pool 'default' created"
else
    echo "✓ Work pool 'default' already exists"
fi

echo ""
echo "Step 2: Deleting old deployments (if any)..."
echo ""

# Delete old deployments (ignore errors if not found)
prefect deployment delete "Hourly Pipeline on YARN/hourly-yarn-pipeline" 2>/dev/null || echo "No old deployment to delete"

echo ""
echo "Step 3: Deploying YARN wrapper flows..."
echo ""

# Deploy hourly pipeline
# Answer both prompts: "Would you like workers to pull code?" (n) and "Save config?" (n)
printf "n\nn\n" | prefect deploy Prefect/yarn_wrapper_flow.py:hourly_pipeline_yarn_flow \
    --name "hourly-yarn-pipeline" \
    --cron "0 * * * *" \
    --pool default \
    --description "Hourly AQI pipeline running on YARN cluster via spark-submit" \
    --tag aqi \
    --tag hourly \
    --tag yarn \
    --tag production

echo ""
echo "=================================================="
echo "✓ Deployment Complete!"
echo "=================================================="
echo ""
echo "Deployed flow:"
echo "  • hourly-yarn-pipeline → Runs every hour on YARN (cron: 0 * * * *)"
echo ""
echo "Next steps:"
echo ""
echo "1. Start Prefect worker:"
echo "   nohup prefect worker start --pool default > logs/prefect-worker.log 2>&1 &"
echo ""
echo "2. Check worker status:"
echo "   ps aux | grep 'prefect worker'"
echo ""
echo "3. View deployments:"
echo "   prefect deployment ls"
echo ""
echo "4. Manual trigger:"
echo "   prefect deployment run 'Hourly Pipeline on YARN/hourly-yarn-pipeline'"
echo ""
echo "5. Monitor runs:"
echo "   prefect flow-run ls --limit 10"
echo ""
echo "6. View logs:"
echo "   tail -f logs/prefect-worker.log"
echo ""
echo "7. Prefect UI:"
echo "   http://localhost:4200"
echo ""
