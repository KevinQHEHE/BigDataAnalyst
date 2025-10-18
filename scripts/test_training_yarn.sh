#!/bin/bash

# Test training script with YARN mode
# Usage: ./scripts/test_training_yarn.sh

echo "=================================================="
echo "Running TEST training with YARN"
echo "  - Sample size: 5000 records"
echo "  - Lightweight tuning: 1 hyperparameter combo"
echo "  - Mode: YARN (cluster)"
echo "  - Output: output_ml/predictions (CSV)"
echo "=================================================="

# Create output directory if not exists
mkdir -p output_ml/predictions

python3 jobs/ml/train_pm25_forecast.py \
    --spark-mode cluster \
    --parallelism 2 \
    --sample-size 100 \
    --min-records 50 \
    --lightweight-tuning \
    --sample-predictions 10 \
    --predictions-output output_ml/predictions \
    --tracking-uri http://localhost:5000 \
    --experiment-name pm25_forecast_yarn_test \
    "$@"

echo "=================================================="
echo "Checking output files in output_ml/predictions:"
ls -lh output_ml/predictions/ || echo "No output files found"
echo "=================================================="

echo "=================================================="
echo "✓ YARN test completed!"
echo "=================================================="
