#!/usr/bin/env python3
"""Export all tables from lakehouse HDFS to CSV files.

This script reads parquet files from HDFS warehouse and exports them as CSV files
to the specified local output directory.

Usage:
    spark-submit --master yarn scripts/export_lakehouse_to_csv.py [output_dir]
    
    If output_dir is not specified, defaults to:
    /home/dlhnhom2/dlh-aqi/data/lakehouse_data
"""
import sys
import os
from pathlib import Path
from datetime import datetime
import subprocess

from pyspark.sql import SparkSession

# Setup paths for imports
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse_aqi.logging_setup import setup as setup_logging

logger = setup_logging()

# Default output directory
DEFAULT_OUTPUT_DIR = "/home/dlhnhom2/dlh-aqi/data/lakehouse_data"

# HDFS warehouse base path
HDFS_WAREHOUSE = "hdfs://khoa-master:9000/warehouse/iceberg/lh"

# Tables to export with their HDFS paths
TABLES_TO_EXPORT = [
    # Bronze layer
    ("bronze/open_meteo_hourly", "hadoop_catalog.lh.bronze.open_meteo_hourly"),
    # Silver layer
    ("silver/air_quality_hourly_clean", "hadoop_catalog.lh.silver.air_quality_hourly_clean"),
    # Gold layer - dimensions
    ("gold/dim_date", "hadoop_catalog.lh.gold.dim_date"),
    ("gold/dim_time", "hadoop_catalog.lh.gold.dim_time"),
    ("gold/dim_location", "hadoop_catalog.lh.gold.dim_location"),
    ("gold/dim_pollutant", "hadoop_catalog.lh.gold.dim_pollutant"),
    # Gold layer - facts
    ("gold/fact_air_quality_hourly", "hadoop_catalog.lh.gold.fact_air_quality_hourly"),
    ("gold/fact_city_daily", "hadoop_catalog.lh.gold.fact_city_daily"),
    ("gold/fact_episode", "hadoop_catalog.lh.gold.fact_episode"),
]


def export_tables_to_csv(spark, output_dir: str) -> None:
    """Export all tables from HDFS to CSV format.
    
    Args:
        spark: SparkSession instance
        output_dir: Directory where CSV files will be saved
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting export of lakehouse tables to: {output_dir}")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    
    success_count = 0
    failed_count = 0
    failed_tables = []
    
    for hdfs_path, table_name in TABLES_TO_EXPORT:
        try:
            logger.info(f"Exporting table: {table_name}")
            
            # Build full HDFS path to parquet files
            full_hdfs_path = f"{HDFS_WAREHOUSE}/{hdfs_path}"
            logger.info(f"  Reading from: {full_hdfs_path}")
            
            # Read parquet from HDFS
            df = spark.read.parquet(full_hdfs_path)
            
            # Get record count
            row_count = df.count()
            logger.info(f"  - Record count: {row_count:,}")
            
            # Generate output file path
            safe_table_name = table_name.replace(".", "_")
            csv_path = output_path / f"{safe_table_name}.csv"
            
            # Convert to pandas and save as CSV
            logger.info(f"  - Converting to pandas and saving to CSV...")
            df_pandas = df.toPandas()
            df_pandas.to_csv(str(csv_path), index=False, encoding='utf-8')
            
            logger.info(f"  ✓ Successfully exported {row_count:,} rows to: {csv_path}")
            success_count += 1
            
        except Exception as e:
            logger.error(f"  ✗ Failed to export {table_name}: {str(e)}")
            failed_tables.append((table_name, str(e)))
            failed_count += 1
    
    # Print summary
    logger.info("\n" + "=" * 80)
    logger.info("EXPORT SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total tables: {len(TABLES_TO_EXPORT)}")
    logger.info(f"Successfully exported: {success_count}")
    logger.info(f"Failed: {failed_count}")
    
    if failed_tables:
        logger.error("\nFailed tables:")
        for table_name, error in failed_tables:
            logger.error(f"  - {table_name}: {error}")
    
    logger.info(f"\nOutput directory: {output_dir}")
    logger.info(f"Export completed at: {datetime.now().isoformat()}")


def main():
    """Main entry point."""
    # Parse arguments
    output_dir = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_DIR
    
    logger.info(f"Initializing Spark session for export job...")
    
    # Build Spark session with Iceberg config
    builder = SparkSession.builder.appName("export_lakehouse_to_csv")
    builder = builder.master("yarn")
    
    builder = (
        builder
        .config("spark.submit.deployMode", "client")
        .config("spark.dynamicAllocation.enabled", "true")
        .config("spark.dynamicAllocation.minExecutors", "1")
        .config("spark.dynamicAllocation.maxExecutors", "50")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.hadoop_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.hadoop_catalog.type", "hadoop")
        .config("spark.sql.catalog.hadoop_catalog.warehouse", os.getenv("WAREHOUSE_URI", "hdfs://khoa-master:9000/warehouse/iceberg"))
        .config("spark.sql.catalogImplementation", "in-memory")
    )
    
    spark = builder.getOrCreate()
    
    try:
        logger.info(f"Spark session created successfully")
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Master: {spark.sparkContext.master}")
        
        # Export tables
        export_tables_to_csv(spark, output_dir)
        
        logger.info("Export job completed successfully!")
        
    except Exception as e:
        logger.error(f"Export job failed with error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()

# Setup paths for imports
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lakehouse_aqi.spark_session import build as build_spark_session
from lakehouse_aqi.logging_setup import setup as setup_logging

logger = setup_logging()

# Default output directory
DEFAULT_OUTPUT_DIR = "/home/dlhnhom2/dlh-aqi/data/lakehouse_data"

# Tables to export (from bronze, silver, and gold layers)
TABLES_TO_EXPORT = [
    # Bronze layer
    "hadoop_catalog.lh.bronze.open_meteo_hourly",
    # Silver layer
    "hadoop_catalog.lh.silver.air_quality_hourly_clean",
    # Gold layer - dimensions
    "hadoop_catalog.lh.gold.dim_date",
    "hadoop_catalog.lh.gold.dim_time",
    "hadoop_catalog.lh.gold.dim_location",
    "hadoop_catalog.lh.gold.dim_pollutant",
    # Gold layer - facts
    "hadoop_catalog.lh.gold.fact_air_quality_hourly",
    "hadoop_catalog.lh.gold.fact_city_daily",
    "hadoop_catalog.lh.gold.fact_episode",
]


def export_tables_to_csv(spark, output_dir: str) -> None:
    """Export all tables to CSV format.
    
    Args:
        spark: SparkSession instance
        output_dir: Directory where CSV files will be saved
    """
    import shutil
    
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Starting export of lakehouse tables to: {output_dir}")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    
    success_count = 0
    failed_count = 0
    failed_tables = []
    
    for table_name in TABLES_TO_EXPORT:
        try:
            logger.info(f"Exporting table: {table_name}")
            
            # Read table from Iceberg catalog
            df = spark.read.table(table_name)
            
            # Get record count
            row_count = df.count()
            logger.info(f"  - Record count: {row_count:,}")
            
            # Generate output file path
            safe_table_name = table_name.replace(".", "_")
            csv_path = output_path / f"{safe_table_name}.csv"
            
            # Use HDFS for temp storage
            hdfs_temp_dir = f"hdfs://khoa-master:9000/tmp/{safe_table_name}_{datetime.now().timestamp()}"
            
            # Export to HDFS parquet first
            df.coalesce(1).write.mode("overwrite").format("parquet").save(hdfs_temp_dir)
            
            # Read back and convert to pandas
            df_pandas = spark.read.parquet(hdfs_temp_dir).toPandas()
            df_pandas.to_csv(str(csv_path), index=False, encoding='utf-8')
            
            # Clean up HDFS temp directory
            import subprocess
            subprocess.run(["hdfs", "dfs", "-rm", "-r", hdfs_temp_dir], 
                         capture_output=True)
            
            logger.info(f"  ✓ Successfully exported {row_count:,} rows to: {csv_path}")
            success_count += 1
            
        except Exception as e:
            logger.error(f"  ✗ Failed to export {table_name}: {str(e)}")
            failed_tables.append((table_name, str(e)))
            failed_count += 1
    
    # Print summary
    logger.info("\n" + "=" * 80)
    logger.info("EXPORT SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total tables: {len(TABLES_TO_EXPORT)}")
    logger.info(f"Successfully exported: {success_count}")
    logger.info(f"Failed: {failed_count}")
    
    if failed_tables:
        logger.error("\nFailed tables:")
        for table_name, error in failed_tables:
            logger.error(f"  - {table_name}: {error}")
    
    logger.info(f"\nOutput directory: {output_dir}")
    logger.info(f"Export completed at: {datetime.now().isoformat()}")


def main():
    """Main entry point."""
    # Parse arguments
    output_dir = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_OUTPUT_DIR
    
    logger.info(f"Initializing Spark session for export job...")
    
    # Build Spark session
    spark = build_spark_session(app_name="export_lakehouse_to_csv")
    
    try:
        logger.info(f"Spark session created successfully")
        logger.info(f"Spark version: {spark.version}")
        logger.info(f"Master: {spark.sparkContext.master}")
        
        # Export tables
        export_tables_to_csv(spark, output_dir)
        
        logger.info("Export job completed successfully!")
        
    except Exception as e:
        logger.error(f"Export job failed with error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
