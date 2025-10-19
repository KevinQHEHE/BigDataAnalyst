#!/bin/bash

# Quick test script for PM2.5 forecast training with reduced data and hyperparameters
# Usage: ./scripts/quick_test_training.sh

echo "=================================================="
echo "Running ULTRA-QUICK TEST training (~30-60 seconds)"
echo "  - Sample size: 100 records"
echo "  - Lightweight tuning: 1 hyperparameter combo"
echo "=================================================="

python3 jobs/ml/train_pm25_forecast.py \
    --spark-mode local \
    --parallelism 1 \
    --sample-size 100 \
    --min-records 50 \
    --lightweight-tuning \
    --sample-predictions 10 \
    --predictions-output output_ml/predictions \
    --tracking-uri http://localhost:5000 \
    --experiment-name pm25_forecast_ultra_quick \
    "$@"

echo "=================================================="
echo "✓ Quick test completed!"
echo "=================================================="
