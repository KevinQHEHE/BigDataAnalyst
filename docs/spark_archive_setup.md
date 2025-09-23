# Pre-staged Spark libraries on HDFS (recommended)

This note documents the recommended way to pre-stage Spark libraries on HDFS for production runs in this repository. After experimenting with a single tar archive approach, we found the most reliable and maintainable option is to pre-stage the Spark JARs and Python files individually and point Spark to them via `spark.yarn.jars` and `--py-files` (exposed in this repo as `SPARK_YARN_JARS` and `SPARK_PYFILES`).

There is an experimental archive method (`spark.yarn.archive`) described below for reference, but it is not the default/recommended path because archives must exactly mirror the Spark distribution layout and are harder to create and maintain.

## Recommended: one-time HDFS staging (individual files)

On the master node (one-time):

```bash
# Create the /spark directory on HDFS and upload JARs + Python libs
hdfs dfs -mkdir -p /spark/jars
hdfs dfs -mkdir -p /spark/python

# From your Spark installation (example)
# Copy jars directory contents
hdfs dfs -put -f $SPARK_HOME/jars/* /spark/jars/

# Upload Python artifacts used by PySpark
hdfs dfs -put -f $SPARK_HOME/python/lib/pyspark.zip /spark/python/
hdfs dfs -put -f $SPARK_HOME/python/lib/py4j-*.zip /spark/python/
```

Configuration in `.env` (example):

```bash
# Use the wildcard of pre-staged jars on HDFS
SPARK_YARN_JARS=hdfs://khoa-master:9000/spark/jars/*
# Provide any python artifacts; the submit wrapper uses these in --py-files
SPARK_PYFILES=hdfs://khoa-master:9000/spark/python/pyspark.zip,hdfs://khoa-master:9000/spark/python/py4j-0.10.9.9-src.zip
```

Why this is preferred
- Reliable: Spark can load individual jars from HDFS without requiring an exact Spark distribution layout inside a tar archive
- Easy to update: replace individual jars when upgrading a single dependency
- Debug-friendly: logs clearly show which files are served from HDFS

## Experimental: single-tar archive (spark.yarn.archive)

You can still use a single tarball archive, but treat it as experimental. The archive must contain a full distribution structure (not just selected files). A typical creation step on the Spark master looks like:

```bash
# From $SPARK_HOME
# tar -C $SPARK_HOME -czf spark-4.0.0-dist.tgz jars python/lib/pyspark.zip python/lib/py4j-0.10.9.9-src.zip
hdfs dfs -mkdir -p /spark
hdfs dfs -put -f spark-4.0.0-dist.tgz /spark/
```

Set in `.env` for testing only:

```bash
# SPARK_YARN_ARCHIVE=hdfs:///spark/spark-4.0.0-dist.tgz
```

Note: We attempted this archive approach during development and observed ClassNotFound errors because the archive's internal layout didn't exactly match what Spark's YARN client expects. If you want to pursue this path, carefully mirror a working Spark distribution and validate the archive on a staging cluster.

## Usage (submit scripts)

This repository provides `scripts/submit_yarn.sh` and `scripts/run_silver_range.sh` which automatically use the `.env` settings. Typical examples:

```bash
# Single run for a date range (clean -> components -> index if using the range script)
bash scripts/run_silver_range.sh --start 2024-01-01T00:00:00 --end 2024-01-31T23:00:00

# Submit a single job directly (clean step)
bash scripts/submit_yarn.sh silver/clean_hourly.py --start 2024-08-01T00:00:00 --end 2024-08-31T23:00:00
```

The wrapper will prefer `SPARK_YARN_ARCHIVE` when set (experimental), otherwise it assembles `--conf spark.yarn.jars=...` and `--py-files` from `SPARK_YARN_JARS` and `SPARK_PYFILES`.

## Troubleshooting

If you still see uploads in the Spark client logs or ClassNotFound errors, check the following:

1. HDFS paths exist and are readable by the submitting user:

```bash
hdfs dfs -ls /spark/jars | head
hdfs dfs -ls /spark/python
```

2. `.env` configuration: prefer `SPARK_YARN_JARS`/`SPARK_PYFILES` for stable runs. Only enable `SPARK_YARN_ARCHIVE` for experimental validation.

3. If using the archive, verify internal structure matches Spark's expectations. Missing classes such as `org.apache.spark.deploy.yarn.ExecutorLauncher` indicate the archive did not include the correct JARs or directory layout.

## Verification commands

```bash
# Count staged jars
hdfs dfs -ls /spark/jars/ | wc -l

# Check python artifacts
hdfs dfs -ls /spark/python/

# Check archive (if present)
hdfs dfs -ls /spark/spark-4.0.0-dist.tgz
```