"""Spark Session Context Manager for Prefect Flows.

This module provides a context manager to ensure only one SparkSession per flow execution
and validates that the session is running on YARN when expected.

Design:
- SparkSessionContext: Context manager that creates/manages a single SparkSession
- Validates master == 'yarn' when required
- Prevents multiple session creation within the same flow
- Properly stops session on exit

Usage:
    with SparkSessionContext(app_name="my_flow", require_yarn=True) as spark:
        # Use spark session
        df = spark.sql("SELECT * FROM table")
"""
import os
import sys
from contextlib import contextmanager
from typing import Optional, Generator
from pathlib import Path

# Setup paths for imports
ROOT_DIR = Path(__file__).resolve().parent.parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from dotenv import load_dotenv
load_dotenv(ROOT_DIR / ".env")

from pyspark.sql import SparkSession


class SparkSessionContext:
    """Context manager for Spark sessions in Prefect flows.
    
    Ensures single session per flow execution and validates YARN deployment.
    """
    
    def __init__(
        self,
        app_name: str = "prefect_flow",
        require_yarn: bool = False,
        mode: Optional[str] = None
    ):
        """Initialize Spark session context.
        
        Args:
            app_name: Application name for Spark
            require_yarn: If True, validates master == 'yarn'
            mode: Optional mode override ('local' for testing, None for production)
        """
        self.app_name = app_name
        self.require_yarn = require_yarn
        self.mode = mode
        self.spark: Optional[SparkSession] = None
        
    def __enter__(self) -> SparkSession:
        """Create and validate Spark session."""
        from lakehouse_aqi import spark_session
        
        # Determine mode: use explicit mode, or infer from environment
        if self.mode is None:
            # If SPARK_MASTER or SPARK_HOME is set, assume cluster mode
            self.mode = "cluster" if (os.getenv("SPARK_MASTER") or os.getenv("SPARK_HOME")) else None
        
        print(f"[SparkSessionContext] Creating session: {self.app_name} (mode={self.mode})")
        
        # Build session
        self.spark = spark_session.build(app_name=self.app_name, mode=self.mode)
        
        # Validate YARN if required
        master = self.spark.sparkContext.master
        print(f"[SparkSessionContext] Master: {master}")
        
        if self.require_yarn and not master.startswith("yarn"):
            raise RuntimeError(
                f"YARN master required but got '{master}'. "
                "Please submit via scripts/spark_submit.sh with --master yarn"
            )
        
        # Log session info
        app_id = self.spark.sparkContext.applicationId
        print(f"[SparkSessionContext] Application ID: {app_id}")
        print(f"[SparkSessionContext] Warehouse: {self.spark.conf.get('spark.sql.catalog.hadoop_catalog.warehouse')}")
        
        return self.spark
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Stop Spark session."""
        if self.spark:
            print(f"[SparkSessionContext] Stopping session: {self.app_name}")
            try:
                self.spark.stop()
            except Exception as e:
                print(f"[SparkSessionContext] Warning: Error stopping session: {e}")
        
        # Don't suppress exceptions
        return False


@contextmanager
def get_spark_session(
    app_name: str = "prefect_flow",
    require_yarn: bool = False,
    mode: Optional[str] = None
) -> Generator[SparkSession, None, None]:
    """Context manager function for Spark sessions.
    
    This is a functional wrapper around SparkSessionContext for simpler usage.
    
    Args:
        app_name: Application name for Spark
        require_yarn: If True, validates master == 'yarn'
        mode: Optional mode override ('local' for testing, None for production)
        
    Yields:
        SparkSession instance
        
    Example:
        with get_spark_session("my_flow", require_yarn=True) as spark:
            df = spark.sql("SELECT * FROM table")
    """
    with SparkSessionContext(app_name, require_yarn, mode) as spark:
        yield spark


def validate_yarn_mode(spark: SparkSession) -> None:
    """Validate that Spark is running on YARN.
    
    Args:
        spark: SparkSession to validate
        
    Raises:
        RuntimeError: If not running on YARN
    """
    master = spark.sparkContext.master
    if not master.startswith("yarn"):
        raise RuntimeError(
            f"Expected YARN master but got '{master}'. "
            "Please submit via: bash scripts/spark_submit.sh <script> -- <args>"
        )
    print(f"✓ Validated YARN mode: {master}")


def log_spark_info(spark: SparkSession, stage: str = "") -> None:
    """Log useful Spark session information.
    
    Args:
        spark: SparkSession to log info for
        stage: Optional stage name for context
    """
    prefix = f"[{stage}] " if stage else ""
    print(f"{prefix}Spark Info:")
    print(f"  Application: {spark.sparkContext.appName}")
    print(f"  Master: {spark.sparkContext.master}")
    print(f"  App ID: {spark.sparkContext.applicationId}")
    print(f"  Warehouse: {spark.conf.get('spark.sql.catalog.hadoop_catalog.warehouse', 'N/A')}")
    print(f"  Dynamic Allocation: {spark.conf.get('spark.dynamicAllocation.enabled', 'N/A')}")
