"""Train a +6 hour PM2.5 forecasting model using PySpark and log to MLflow.

The script keeps existing project files untouched by introducing a new
standalone training job. It prepares a supervised dataset from the silver
Iceberg table, fits a Gradient Boosted Trees model with hyperparameter tuning,
evaluates on a chronological holdout, and registers metrics/artifacts in MLflow.

Example:
  python jobs/ml/train_pm25_forecast.py \\
      --tracking-uri http://localhost:5000 \\
      --experiment-name pm25_forecast
"""
from __future__ import annotations

import argparse
import math
import os
import sys
import tempfile
from typing import Dict, List, Tuple

import mlflow
import mlflow.spark
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import (
    col,
    dayofweek,
    hour,
    lead,
    lit,
    percent_rank,
    sin,
    cos,
    when,
    abs as spark_abs,
    avg as spark_avg,
    max as spark_max,
    min as spark_min,
    to_timestamp,
)

# Ensure src/ modules are importable without altering existing packages
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

from dotenv import load_dotenv

load_dotenv(os.path.join(ROOT_DIR, ".env"))

from lakehouse_aqi import spark_session  # noqa: E402

BASE_FEATURES = [
    "aod",
    "co",
    "co2",
    "dust",
    "no2",
    "o3",
    "pm10",
    "so2",
    "uv_index",
]


def build_spark_session(app_name: str, mode: str = "auto") -> SparkSession:
    """Build a SparkSession in local or cluster mode without modifying existing code."""
    resolved_mode = None
    if mode == "auto":
        resolved_mode = "cluster" if os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME") else "local"
    else:
        resolved_mode = mode

    spark = spark_session.build(app_name=app_name, mode=resolved_mode)
    spark.conf.set("spark.sql.adaptive.enabled", os.getenv("SPARK_ENABLE_AQE", "true"))
    return spark


def prepare_dataset(
    spark: SparkSession,
    silver_table: str,
    horizon_hours: int,
    min_records: int,
) -> Tuple[DataFrame, List[str]]:
    """Load the silver table and craft features for +6 hour forecasting."""
    df = (
        spark.table(silver_table)
        .select("location_key", "ts_utc", "pm25", *BASE_FEATURES)
        .dropna(subset=["pm25", *BASE_FEATURES])
    )

    if df.count() < min_records:
        raise RuntimeError(
            f"Insufficient samples in {silver_table}: need at least {min_records} complete rows."
        )

    if dict(df.dtypes).get("ts_utc") != "timestamp":
        df = df.withColumn("ts_utc", to_timestamp(col("ts_utc")))

    window = Window.partitionBy("location_key").orderBy(col("ts_utc"))
    df = df.withColumn("label", lead(col("pm25"), horizon_hours).over(window))
    df = df.filter(col("label").isNotNull())

    df = df.withColumn("hour", hour(col("ts_utc")))
    df = df.withColumn("hour_sin", sin(col("hour") * lit(2.0 * math.pi / 24.0)))
    df = df.withColumn("hour_cos", cos(col("hour") * lit(2.0 * math.pi / 24.0)))
    df = df.withColumn("day_of_week", dayofweek(col("ts_utc")))
    df = df.withColumn("dow_sin", sin(col("day_of_week") * lit(2.0 * math.pi / 7.0)))
    df = df.withColumn("dow_cos", cos(col("day_of_week") * lit(2.0 * math.pi / 7.0)))

    feature_columns = BASE_FEATURES + ["hour_sin", "hour_cos", "dow_sin", "dow_cos"]
    df = df.dropna(subset=feature_columns + ["label"])
    return df, feature_columns


def chronological_split(df: DataFrame, train_ratio: float) -> Tuple[DataFrame, DataFrame]:
    """Split train/test chronologically to avoid leakage."""
    timeline = Window.orderBy(col("ts_utc"))
    ranked = df.withColumn("ts_rank", percent_rank().over(timeline))
    train_df = ranked.filter(col("ts_rank") <= train_ratio).drop("ts_rank")
    test_df = ranked.filter(col("ts_rank") > train_ratio).drop("ts_rank")
    return train_df.cache(), test_df.cache()


def build_pipeline(feature_columns: List[str], seed: int):
    """Create ML pipeline and hyperparameter grid."""
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="features_unscaled",
        handleInvalid="skip",
    )
    scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withMean=True, withStd=True)
    gbt = GBTRegressor(
        labelCol="label",
        featuresCol="features",
        predictionCol="prediction",
        maxDepth=8,
        maxIter=120,
        stepSize=0.1,
        seed=seed,
    )

    pipeline = Pipeline(stages=[assembler, scaler, gbt])
    param_grid = (
        ParamGridBuilder()
        .addGrid(gbt.maxDepth, [6, 8])
        .addGrid(gbt.maxIter, [80, 120])
        .addGrid(gbt.stepSize, [0.05, 0.1])
        .build()
    )
    choices = {"maxDepth": [6, 8], "maxIter": [80, 120], "stepSize": [0.05, 0.1]}
    return pipeline, param_grid, choices


def evaluate(predictions: DataFrame) -> Dict[str, float]:
    """Return regression metrics for the holdout set."""
    metrics = {}
    for metric in ("rmse", "mae", "r2"):
        evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName=metric)
        metrics[metric] = float(evaluator.evaluate(predictions))

    mape_df = predictions.select(
        when(col("label") != 0, spark_abs(col("label") - col("prediction")) / spark_abs(col("label"))).alias("ape")
    )
    mape_df = mape_df.dropna(subset=["ape"])
    mape_value = mape_df.agg(spark_avg("ape")).collect()[0][0]
    metrics["mape"] = float(mape_value * 100.0) if mape_value is not None else float("nan")
    return metrics


def log_feature_importances(feature_columns: List[str], importances) -> List[Dict[str, float]]:
    """Convert Spark importance vector to a sorted list for MLflow storage."""
    items = [
        {"feature": name, "importance": float(round(value, 6))}
        for name, value in zip(feature_columns, importances.toArray().tolist())
    ]
    items.sort(key=lambda row: row["importance"], reverse=True)
    return items


def run_training(args: argparse.Namespace) -> Dict[str, object]:
    """Execute the end-to-end training and MLflow logging pipeline."""
    if args.horizon_hours <= 0:
        raise ValueError("horizon-hours must be positive.")
    if not (0.5 < args.train_ratio < 0.95):
        raise ValueError("train-ratio should be between 0.5 and 0.95.")

    if args.tracking_uri:
        mlflow.set_tracking_uri(args.tracking_uri)
    mlflow.set_experiment(args.experiment_name)

    spark = build_spark_session(app_name="pm25_forecast_training", mode=args.spark_mode)
    train_df = test_df = predictions = None

    try:
        dataset, feature_columns = prepare_dataset(
            spark=spark,
            silver_table=args.silver_table,
            horizon_hours=args.horizon_hours,
            min_records=args.min_records,
        )

        train_df, test_df = chronological_split(dataset, args.train_ratio)
        train_rows = train_df.count()
        test_rows = test_df.count()
        if train_rows == 0 or test_rows == 0:
            raise RuntimeError("Train/test split returned zero rows. Verify the silver dataset coverage.")

        pipeline, param_grid, choices = build_pipeline(feature_columns, args.seed)
        evaluator = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
        tuner = TrainValidationSplit(
            estimator=pipeline,
            estimatorParamMaps=param_grid,
            evaluator=evaluator,
            trainRatio=0.85,
            parallelism=args.parallelism,
            seed=args.seed,
        )

        with mlflow.start_run(run_name="pm25_forecast_gbt") as run:
            mlflow.log_params(
                {
                    "silver_table": args.silver_table,
                    "horizon_hours": args.horizon_hours,
                    "train_ratio": args.train_ratio,
                    "min_records": args.min_records,
                    "feature_list": ",".join(feature_columns),
                    "temporal_split": "percent_rank_global_ts",
                    "param_grid_maxDepth": ",".join(map(str, choices["maxDepth"])),
                    "param_grid_maxIter": ",".join(map(str, choices["maxIter"])),
                    "param_grid_stepSize": ",".join(map(str, choices["stepSize"])),
                    "spark_mode": args.spark_mode,
                }
            )

            coverage = dataset.agg(
                spark_min("ts_utc").alias("min_ts"),
                spark_max("ts_utc").alias("max_ts"),
            ).collect()[0]
            mlflow.log_param("data_min_ts", str(coverage["min_ts"]))
            mlflow.log_param("data_max_ts", str(coverage["max_ts"]))

            tuned = tuner.fit(train_df)
            best_model = tuned.bestModel
            gbt_stage: GBTRegressor = best_model.stages[-1]
            mlflow.log_params(
                {
                    "best_maxDepth": gbt_stage.getOrDefault("maxDepth"),
                    "best_maxIter": gbt_stage.getOrDefault("maxIter"),
                    "best_stepSize": gbt_stage.getOrDefault("stepSize"),
                    "best_subsamplingRate": gbt_stage.getOrDefault("subsamplingRate"),
                }
            )

            predictions = best_model.transform(test_df).cache()
            metrics = evaluate(predictions)
            for name, value in metrics.items():
                mlflow.log_metric(f"test_{name}", value)

            mlflow.log_metric("train_row_count", float(train_rows))
            mlflow.log_metric("test_row_count", float(test_rows))

            importances = log_feature_importances(feature_columns, gbt_stage.featureImportances)
            mlflow.log_dict({"feature_importances": importances}, artifact_file="artifacts/feature_importances.json")

            latest = (
                predictions.select("location_key", "ts_utc", "label", "prediction")
                .orderBy(col("ts_utc").desc())
                .limit(max(10, args.sample_predictions))
            )
            with tempfile.TemporaryDirectory() as tmpdir:
                sample_path = os.path.join(tmpdir, "latest_predictions.csv")
                latest.toPandas().to_csv(sample_path, index=False)
                mlflow.log_artifact(sample_path, artifact_path="artifacts")

            if args.predictions_output:
                selected_cols = ["location_key", "ts_utc", "label", "prediction"] + feature_columns
                predictions.select(*selected_cols).coalesce(1).write.mode("overwrite").option("header", "true").csv(
                    args.predictions_output
                )
                mlflow.log_param("predictions_output_path", args.predictions_output)

            mlflow.spark.log_model(best_model, artifact_path="model")

            print("=" * 70)
            print(f"MLflow run_id = {run.info.run_id}")
            print("Evaluation metrics:")
            for name, value in metrics.items():
                print(f"  {name}: {value:.4f}")
            print("=" * 70)

            return {
                "run_id": run.info.run_id,
                "metrics": metrics,
                "train_rows": train_rows,
                "test_rows": test_rows,
                "feature_importances": importances,
                "experiment_name": args.experiment_name,
                "predictions_output": args.predictions_output,
            }
    finally:
        if predictions is not None:
            predictions.unpersist()
        if train_df is not None:
            train_df.unpersist()
        if test_df is not None:
            test_df.unpersist()
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train a modern PySpark Gradient Boosted Trees regressor for +6h PM2.5 forecasting."
    )
    parser.add_argument(
        "--silver-table",
        default="hadoop_catalog.lh.silver.air_quality_hourly_clean",
        help="Target silver table to read training data from.",
    )
    parser.add_argument(
        "--horizon-hours",
        type=int,
        default=6,
        help="Prediction horizon in hours (default 6).",
    )
    parser.add_argument(
        "--train-ratio",
        type=float,
        default=0.8,
        help="Fraction of historical timeline for training (rest used for holdout).",
    )
    parser.add_argument(
        "--min-records",
        type=int,
        default=5000,
        help="Minimum number of complete rows required to run training.",
    )
    parser.add_argument(
        "--experiment-name",
        default="pm25_forecast",
        help="MLflow experiment name.",
    )
    parser.add_argument(
        "--tracking-uri",
        help="MLflow tracking URI, e.g., http://localhost:5000.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility.",
    )
    parser.add_argument(
        "--parallelism",
        type=int,
        default=2,
        help="Parallelism for TrainValidationSplit.",
    )
    parser.add_argument(
        "--sample-predictions",
        type=int,
        default=50,
        help="Number of latest predictions to log as artifact.",
    )
    parser.add_argument(
        "--predictions-output",
        help="Optional path (local or distributed FS) to write full test predictions as CSV directory.",
    )
    parser.add_argument(
        "--spark-mode",
        choices=["auto", "local", "cluster"],
        default="auto",
        help="Spark mode passed to lakehouse_aqi.spark_session.build (default auto).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_training(args)


if __name__ == "__main__":
    main()
