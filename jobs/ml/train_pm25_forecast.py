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
import glob
import math
import os
import shutil
import sys
import tempfile
from typing import Dict, List, Tuple

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

try:
    import mlflow
    import mlflow.spark
    from pyspark.ml import Pipeline
    from pyspark.ml.evaluation import RegressionEvaluator
    from pyspark.ml.feature import StandardScaler, VectorAssembler
    from pyspark.ml.regression import GBTRegressor
    from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit
    from pyspark.sql import DataFrame, SparkSession, Window
    from pyspark.sql.functions import (
        col, dayofweek, hour, lead, lit, percent_rank, sin, cos, when,
        abs as spark_abs, avg as spark_avg, max as spark_max, min as spark_min,
        to_timestamp,
    )
    logger.info("✓ All imports successful")
except Exception as e:
    logger.error(f"✗ Import failed: {e}", exc_info=True)
    sys.exit(1)

# Ensure src/ modules are importable
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
SRC_DIR = os.path.join(ROOT_DIR, "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

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


def build_spark_session(app_name: str = "PM25_Forecast_Training", mode: str = "YARN"):
    """Build PySpark session with Iceberg catalog support."""
    logger.info(f"Building Spark session: {app_name} (mode={mode})")
    resolved_mode = os.getenv("SPARK_MODE", mode).upper()
    
    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
        .config("spark.sql.catalog.hadoop_catalog.warehouse", "hdfs://khoa-master:9000/lakehouse/iceberg-warehouse")
        .config("spark.sql.defaultCatalog", "hadoop_catalog")
        # Compression for shuffle performance
        .config("spark.shuffle.compress", "true")
        .config("spark.shuffle.spill.compress", "true")
        .config("spark.rdd.compress", "true")
        .config("spark.io.compression.codec", "snappy")
        # Query optimization
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "32")
        .config("spark.sql.windowExec.buffer.in.memory.threshold", "10000")
        # Reduce executor overhead
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "1")
        .config("spark.dynamicAllocation.maxExecutors", "4")
        .config("spark.dynamicAllocation.initialExecutors", "2")
        .config("spark.dynamicAllocation.executorIdleTimeout", "30s")
    )
    
    spark = builder.master("yarn" if resolved_mode == "YARN" else "local[*]").getOrCreate()
    logger.info(f"✓ Spark session built (mode={resolved_mode})")
    return spark


def prepare_dataset(
    spark: SparkSession,
    silver_table: str,
    horizon_hours: int,
    min_records: int,
    sample_size: int = None,
) -> Tuple[DataFrame, List[str]]:
    """Load silver table and create features for +6h PM2.5 forecasting."""
    logger.info(f"Loading data from: {silver_table}")
    
    # Try to load table with fallback
    try:
        if silver_table.startswith("hadoop_catalog."):
            df = spark.table(silver_table)
        elif silver_table.startswith("hdfs://"):
            df = spark.read.format("iceberg").load(silver_table)
        else:
            df = spark.read.format("iceberg").load(f"hdfs://khoa-master:9000/warehouse/iceberg/{silver_table}")
    except Exception as e:
        logger.warning(f"Load failed: {e}. Using fallback path.")
        df = spark.read.format("iceberg").load("hdfs://khoa-master:9000/warehouse/iceberg/lh/silver/air_quality_hourly_clean")
    
    # Select and clean data in single operation
    df = df.select("location_key", "ts_utc", "pm25", *BASE_FEATURES).dropna(subset=["pm25", *BASE_FEATURES])
    
    total_count = df.count()
    if total_count < min_records:
        raise RuntimeError(f"Insufficient samples: {total_count} < {min_records}")
    
    # Optional sampling
    if sample_size and sample_size > 0:
        sample_fraction = min(1.0, sample_size / total_count)
        logger.info(f"Sampling {sample_size} from {total_count} (fraction={sample_fraction:.4f})")
        df = df.sample(fraction=sample_fraction, seed=42)
    
    # Convert timestamp and create label
    if dict(df.dtypes).get("ts_utc") != "timestamp":
        df = df.withColumn("ts_utc", to_timestamp(col("ts_utc")))
    
    window = Window.partitionBy("location_key").orderBy(col("ts_utc"))
    df = df.withColumn("label", lead(col("pm25"), horizon_hours).over(window)).filter(col("label").isNotNull())
    
    # Create temporal features efficiently
    df = df.withColumn("hour", hour(col("ts_utc")))
    df = df.withColumn("hour_sin", sin(col("hour") * lit(2.0 * math.pi / 24.0)))
    df = df.withColumn("hour_cos", cos(col("hour") * lit(2.0 * math.pi / 24.0)))
    df = df.withColumn("day_of_week", dayofweek(col("ts_utc")))
    df = df.withColumn("dow_sin", sin(col("day_of_week") * lit(2.0 * math.pi / 7.0)))
    df = df.withColumn("dow_cos", cos(col("day_of_week") * lit(2.0 * math.pi / 7.0)))
    
    feature_columns = BASE_FEATURES + ["hour_sin", "hour_cos", "dow_sin", "dow_cos"]
    return df.dropna(subset=feature_columns + ["label"]), feature_columns


def chronological_split(df: DataFrame, train_ratio: float) -> Tuple[DataFrame, DataFrame]:
    """Split train/test chronologically to avoid leakage.
    
    Partitions by location_key to prevent moving all data to a single partition
    while maintaining temporal ordering within each location.
    """
    timeline = Window.partitionBy("location_key").orderBy(col("ts_utc"))
    ranked = df.withColumn("ts_rank", percent_rank().over(timeline))
    train_df = ranked.filter(col("ts_rank") <= train_ratio).drop("ts_rank")
    test_df = ranked.filter(col("ts_rank") > train_ratio).drop("ts_rank")
    return train_df.cache(), test_df.cache()


def build_pipeline(feature_columns: List[str], seed: int, lightweight_tuning: bool = False):
    """Create ML pipeline and hyperparameter grid."""
    gbt_params = {"labelCol": "label", "featuresCol": "features", "predictionCol": "prediction", "seed": seed}
    if lightweight_tuning:
        gbt_params.update({"maxDepth": 6, "maxIter": 50, "stepSize": 0.1})
    else:
        gbt_params.update({"maxDepth": 8, "maxIter": 120, "stepSize": 0.1})
    
    gbt = GBTRegressor(**gbt_params)
    pipeline = Pipeline(stages=[
        VectorAssembler(inputCols=feature_columns, outputCol="features_unscaled", handleInvalid="skip"),
        StandardScaler(inputCol="features_unscaled", outputCol="features", withMean=True, withStd=True),
        gbt
    ])
    
    if lightweight_tuning:
        param_grid = ParamGridBuilder().addGrid(gbt.maxDepth, [6]).build()
        choices = {"maxDepth": [6], "maxIter": [50], "stepSize": [0.1]}
    else:
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
        metrics[metric] = float(RegressionEvaluator(
            labelCol="label", predictionCol="prediction", metricName=metric
        ).evaluate(predictions))
    
    # Calculate MAPE
    mape_df = predictions.select(
        when(col("label") != 0, spark_abs(col("label") - col("prediction")) / spark_abs(col("label"))).alias("ape")
    ).dropna(subset=["ape"])
    
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
            sample_size=args.sample_size,
        )

        train_df, test_df = chronological_split(dataset, args.train_ratio)
        train_rows = train_df.count()
        test_rows = test_df.count()
        if train_rows == 0 or test_rows == 0:
            raise RuntimeError("Train/test split returned zero rows. Verify the silver dataset coverage.")

        pipeline, param_grid, choices = build_pipeline(feature_columns, args.seed, lightweight_tuning=args.lightweight_tuning)
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
                    "sample_size": str(args.sample_size) if args.sample_size else "None",
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

            if args.predictions_single_csv:
                if "://" in args.predictions_single_csv:
                    raise ValueError(
                        "--predictions-single-csv only supports local filesystem paths. "
                        "For distributed/remote storage use --predictions-output."
                    )
                temp_dir = tempfile.mkdtemp(prefix="pm25_predictions_csv_")
                try:
                    selected_cols = ["location_key", "ts_utc", "label", "prediction"] + feature_columns
                    (
                        predictions.select(*selected_cols)
                        .coalesce(1)
                        .write.mode("overwrite")
                        .option("header", "true")
                        .csv(temp_dir)
                    )
                    part_files = glob.glob(os.path.join(temp_dir, "part-*"))
                    if part_files:
                        output_dir = os.path.dirname(args.predictions_single_csv)
                        if output_dir:
                            os.makedirs(output_dir, exist_ok=True)
                        shutil.move(part_files[0], args.predictions_single_csv)
                    else:
                        logger.warning("Spark did not materialize part files for predictions; falling back to toPandas().")
                        pdf = predictions.select(*selected_cols).toPandas()
                        output_dir = os.path.dirname(args.predictions_single_csv)
                        if output_dir:
                            os.makedirs(output_dir, exist_ok=True)
                        pdf.to_csv(args.predictions_single_csv, index=False)

                    mlflow.log_param("predictions_single_csv_path", args.predictions_single_csv)
                    mlflow.log_artifact(args.predictions_single_csv, artifact_path="artifacts/full_predictions")
                finally:
                    shutil.rmtree(temp_dir, ignore_errors=True)

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
                "predictions_single_csv": args.predictions_single_csv,
            }
    except Exception as e:
        logger.error(f"Training pipeline failed: {e}", exc_info=True)
        raise
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
        "--sample-size",
        type=int,
        default=None,
        help="Optional: subsample to this many records for quick testing (None = use all data).",
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
        "--predictions-single-csv",
        help="Optional local file path to write full test predictions as a single CSV file.",
    )
    parser.add_argument(
        "--spark-mode",
        choices=["auto", "local", "cluster"],
        default="auto",
        help="Spark mode passed to lakehouse_aqi.spark_session.build (default auto).",
    )
    parser.add_argument(
        "--lightweight-tuning",
        action="store_true",
        help="Use minimal hyperparameter grid for quick testing (1 job instead of 12).",
    )
    return parser.parse_args()


def main() -> None:
    try:
        args = parse_args()
        logger.info(f"Training arguments: {args}")
        run_training(args)
    except Exception as e:
        logger.error(f"Training failed with error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
