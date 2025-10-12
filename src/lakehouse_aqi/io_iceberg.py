"""Helpers for writing to Iceberg tables (append + upsert patterns).

These helpers expect a SparkSession passed in and use Spark SQL MERGE INTO when keys are provided.
"""
from typing import List, Optional
from pyspark.sql import DataFrame, SparkSession


def append_table(df: DataFrame, table: str) -> None:
    """Append a DataFrame to an Iceberg table using DataFrameWriter.

    Args:
        df: DataFrame to write
        table: Iceberg table identifier like 'hadoop_catalog.aq.s_clean_hourly'
    """
    df.write.format("iceberg").mode("append").saveAsTable(table)


def merge_into(spark: SparkSession, df: DataFrame, table: str, keys: List[str]) -> None:
    """Perform an upsert (merge) into Iceberg table. This creates a temp view and runs MERGE SQL.

    Args:
        spark: SparkSession
        df: DataFrame to upsert
        table: fully-qualified Iceberg table
        keys: list of key columns to dedupe/join on
    """
    if not keys:
        raise ValueError("keys must be provided for merge_into")

    tmp_view = "__tmp_upsert_view"
    df.createOrReplaceTempView(tmp_view)

    # Build join condition
    conds = [f"t.{k} = s.{k}" for k in keys]
    on_clause = " AND ".join(conds)

    # Build update set for non-key columns
    non_keys = [c for c in df.columns if c not in keys]
    update_set = ", ".join([f"{c} = s.{c}" for c in non_keys]) if non_keys else ""

    merge_sql = f"""
    MERGE INTO {table} t
    USING {tmp_view} s
    ON {on_clause}
    WHEN MATCHED THEN UPDATE SET {update_set}
    WHEN NOT MATCHED THEN INSERT *
    """

    spark.sql(merge_sql)
