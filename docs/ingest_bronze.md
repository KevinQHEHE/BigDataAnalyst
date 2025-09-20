Ingesting data into Bronze (open-meteo air quality)

This document describes the recommended process to ingest hourly air-quality data from Open-Meteo into the Bronze table `hadoop_catalog.aq.raw_open_meteo_hourly` (Iceberg), including examples, CLI options, SQL checks and maintenance notes.

Prerequisites
- Spark with Iceberg runtime available on the cluster.
- HDFS NameNode writable (not in safe mode).
- `configs/locations.json` present with locations in the format:
  {
    "location_name": {"latitude": 21.0, "longitude": 105.8}
  }
- `script/submit_yarn.sh` wrapper in the repository root and the job at `jobs/ingest/open_meteo_bronze.py`.

Disable HDFS safe mode (if necessary)
Run the following to check and leave safe mode:

```bash
hdfs dfsadmin -safemode get
hdfs dfsadmin -safemode leave
```

Spark-submit examples

1) Ingest specific date range into Bronze
Fetch locations from `configs/locations.json` for the date range 2024-01-01 -> 2024-01-31, splitting the work into 10-day chunks to avoid API limits:

```bash
bash script/submit_yarn.sh ingest/open_meteo_bronze.py \
  --locations configs/locations.json \
  --start-date 2024-01-01 \
  --end-date 2024-01-31 \
  --chunk-days 10
```

2) Update from latest date in DB to now (auto-backfill if empty)
This will compute the start date from `MAX(ts)` in `hadoop_catalog.aq.raw_open_mete_hourly` and fetch from that timestamp until now. If the table is empty, the script will prompt to confirm a backfill from 2023-01-01. Use `--yes` to auto-accept.

Interactive (will prompt if table empty):

```bash
bash script/submit_yarn.sh ingest/open_meteo_bronze.py \
  --locations configs/locations.json \
  --update-from-db \
  --chunk-days 10
```

Non-interactive (auto-accept backfill):

```bash
bash script/submit_yarn.sh ingest/open_meteo_bronze.py \
  --locations configs/locations.json \
  --update-from-db --yes \
  --chunk-days 10
```

CLI options (job arguments)

- `--locations`
  - Type: path (string)
  - Required: yes
  - Description: Path to a JSON file containing locations in the shape `{name: {"latitude": ..., "longitude": ...}}`.
  - Example: `configs/locations.json`

- `--start-date`
  - Type: date string `YYYY-MM-DD` (UTC)
  - Required: not required when `--update-from-db` is used. Otherwise required.
  - Description: Inclusive start date for fetching data.
  - Example: `2024-01-01`

- `--end-date`
  - Type: date string `YYYY-MM-DD` (UTC)
  - Required: not required when `--update-from-db` is used. Otherwise required.
  - Description: Inclusive end date for fetching data.
  - Example: `2024-01-31`

- `--chunk-days`
  - Type: int
  - Default: `10`
  - Description: Split the date range into chunks of at most N days per API call to avoid large payloads.

- `--update-from-db`
  - Type: flag (boolean)
  - Default: false
  - Description: Compute the start date from `MAX(ts)` in the table `hadoop_catalog.aq.raw_open_meteo_hourly` and fetch from that timestamp to now. If the table is empty, the script asks whether to backfill from `2023-01-01`.

- `--yes`
  - Type: flag (boolean)
  - Default: false
  - Description: Automatically accept interactive prompts. Use with `--update-from-db` in non-interactive environments or CI to auto-confirm backfill.

SQL checks for Bronze

Start a spark-sql shell configured with the Iceberg `hadoop_catalog` and the warehouse URI (example uses local mode for quick checks):

```bash
SPARK_HOME=${SPARK_HOME:-/home/dlhnhom2/spark} \
$SPARK_HOME/bin/spark-sql --master local[1] \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop \
  --conf spark.sql.catalog.hadoop_catalog.warehouse=hdfs://khoa-master:9000/warehouse/iceberg
```

List tables in the namespace:

```sql
SHOW TABLES IN hadoop_catalog.aq;
```

Count total rows:

```sql
SELECT COUNT(*) AS total_rows FROM hadoop_catalog.aq.raw_open_meteo_hourly;
```

Sample rows:

```sql
SELECT * FROM hadoop_catalog.aq.raw_open_meteo_hourly ORDER BY ts DESC LIMIT 20;
```

Global time range (min/max):

```sql
SELECT MIN(ts) AS min_ts, MAX(ts) AS max_ts FROM hadoop_catalog.aq.raw_open_meteo_hourly;
```

Time range per location:

```sql
SELECT location_id, MIN(ts) AS min_ts, MAX(ts) AS max_ts
FROM hadoop_catalog.aq.raw_open_mete_hourly
GROUP BY location_id
ORDER BY location_id;
```

Record counts per location:

```sql
SELECT location_id, COUNT(*) AS total_records
FROM hadoop_catalog.aq.raw_open_meteo_hourly
GROUP BY location_id
ORDER BY total_records DESC;
```

Find duplicate records by (ts, location_id):

```sql
WITH dup AS (
  SELECT ts, location_id, COUNT(*) AS cnt
  FROM hadoop_catalog.aq.raw_open_meteo_hourly
  GROUP BY ts, location_id
  HAVING COUNT(*) > 1
)
SELECT t.*
FROM hadoop_catalog.aq.raw_open_meteo_hourly t
JOIN dup d ON t.ts = d.ts AND t.location_id = d.location_id
ORDER BY t.location_id, t.ts;
```

WARNING — delete/reset (only when you mean to re-ingest)

Truncate entire Iceberg table (fast):

```sql
TRUNCATE TABLE hadoop_catalog.aq.raw_open_meteo_hourly;
```

Or delete all rows (alternative):

```sql
DELETE FROM hadoop_catalog.aq.raw_open_meteo_hourly WHERE TRUE;
```

Cleanup & compacting WSL2 / Docker Desktop VHDX (optional)

1) Remove temporary files on Linux host (example):

```bash
sudo rm -rf /tmp/* /var/tmp/* \
             /var/log/hadoop-yarn/containers/* \
             /var/hadoop/yarn/local/usercache/*/*
```

2) Create a zero-filled file then remove to free space in WSL2 ext4 image (example):

```bash
sudo dd if=/dev/zero of=~/zero.fill bs=1M status=progress || true
sync
sudo rm -f ~/zero.fill
```

3) Shutdown WSL and optimize the VHDX using PowerShell on Windows host:

```powershell
wsl --shutdown
Optimize-VHD -Path "E:\Docker\DockerDesktopWSL\main\ext4.vhdx" -Mode Full
Optimize-VHD -Path "E:\Docker\DockerDesktopWSL\disk\docker_data.vhdx" -Mode Full
```

Notes and recommendations
- The job deduplicates by `(location_id, ts)` before writing to Bronze: `df.dropDuplicates(["location_id","ts"])`.
- The script replaces negative pollutant values with `NULL` to keep Bronze "raw-normalized".
- The order of `HOURLY_VARS` is important because the Open-Meteo client maps values by index.
- Set `SPARK_HOME` or `SPARK_SUBMIT` if your environment differs from the defaults in `script/submit_yarn.sh`.
- Consider adding `--backfill-start` to the job if you want to control the backfill earliest date (currently hard-coded to `2023-01-01`).

---

Generated by the repository helper tooling. If you want this file in a different format (README.md, rst) or translated back to Vietnamese, tell me and I'll add it to `docs/`.
