# System Report

## Host Overview
- Generated: 2025-09-17 15:02:36Z (UTC)
- Kernel: Linux khoa-master 6.6.87.2-microsoft-standard-WSL2 #1 SMP PREEMPT_DYNAMIC Thu Jun  5 18:30:46 UTC 2025 x86_64 x86_64 x86_64 GNU/Linux
- OS: Ubuntu 22.04.5 LTS (Jammy Jellyfish)
- Environment: WSL2 on Microsoft hypervisor (systemd unavailable)

## Hardware Summary
- CPU: Intel(R) Core(TM) i7-10870H CPU @ 2.20GHz (16 vCPU, 8 cores / 2 threads per core)
- Virtualization: VT-x capable, running under Microsoft hypervisor
- Memory: 15 GiB total, 4 GiB swap
- Cache: L1d 256 KiB ×8, L1i 256 KiB ×8, L2 2 MiB ×8, L3 16 MiB ×1

```
Filesystem      Size  Used Avail Use% Mounted on
overlay        1007G   22G  935G   3% /
tmpfs            64M     0   64M   0% /dev
shm              64M     0   64M   0% /dev/shm
/dev/sde       1007G   22G  935G   3% /etc/hosts
tmpfs           7.8G     0  7.8G   0% /proc/acpi
tmpfs           7.8G     0  7.8G   0% /proc/scsi
tmpfs           7.8G     0  7.8G   0% /sys/firmware
```

### Running Processes (`top -bn1 | head -n 15`)
```
top - 14:28:29 up  1:25,  0 users,  load average: 0.14, 0.43, 0.31
Tasks:  32 total,   1 running,  20 sleeping,   0 stopped,  11 zombie
%Cpu(s):  0.8 us,  0.0 sy,  0.0 ni, 99.2 id,  0.0 wa,  0.0 hi,  0.0 si,  0.0 st
MiB Mem :  15906.2 total,  10868.3 free,   3734.6 used,   1303.2 buff/cache
MiB Swap:   4096.0 total,   4096.0 free,      0.0 used.  11925.0 avail Mem 

  PID USER      PR  NI    VIRT    RES    SHR S  %CPU  %MEM     TIME+ COMMAND
    1 root      20   0    2824   1408   1408 S   0.0   0.0   0:00.04 tail
   16 root      20   0   15436   5456   3840 S   0.0   0.0   0:00.00 sshd
  152 dlhnhom2  20   0       0      0      0 Z   0.0   0.0   0:00.00 sshd
  284 dlhnhom2  20   0       0      0      0 Z   0.0   0.0   0:00.00 sshd
  685 dlhnhom2  20   0       0      0      0 Z   0.0   0.0   0:00.00 sshd
  714 dlhnhom2  20   0 6046716 327140  24772 S   0.0   2.0   0:09.64 java
  930 dlhnhom2  20   0       0      0      0 Z   0.0   0.0   0:00.00 sshd
  959 dlhnhom2  20   0 6014416 229832  24328 S   0.0   1.4   0:06.82 java
```

## Storage Layout (`lsblk`)
```
NAME  MAJ:MIN RM   SIZE RO TYPE MOUNTPOINTS
loop0   7:0    0   597M  1 loop 
loop1   7:1    0 646.7M  1 loop 
sda     8:0    0 388.4M  1 disk 
sdb     8:16   0   186M  1 disk 
sdc     8:32   0     4G  0 disk [SWAP]
sdd     8:48   0     1T  0 disk 
sde     8:64   0     1T  0 disk /etc/hosts
                                /etc/hostname
                                /etc/resolv.conf
```

## Network Interfaces (`ip -brief addr`)
```
lo               UNKNOWN        127.0.0.1/8 ::1/128 
eth0@if45        UP             172.18.0.3/16 
```

## Tool Versions
- `bash`: GNU bash 5.1.16
- `python3`: Python 3.10.12
- `pip3`: pip 22.0.2 (python 3.10)
- `java`: OpenJDK 21.0.8+9 (Ubuntu)

## Missing Common Tools
- `git`: not found (likely not installed in this environment)
- `node`: not found
- `docker`: not found

## Health Notes
- `systemctl --failed` unavailable because this WSL2 instance does not use systemd as PID 1.
- Load averages and resource usage are low; plenty of free memory and disk space available.
- Multiple zombie `sshd` processes remain (owned by PID 1); full cleanup will require restarting the host SSH daemon with elevated privileges.

## Lakehouse Stack Status (Air Quality Analytics)

### Hadoop (HDFS & YARN)
- Hadoop 3.4.1 available at `/home/dlhnhom2/hadoop` (`HADOOP_HOME` exported).
- Core services active on this host: `NameNode`, `ResourceManager`, `SecondaryNameNode`; three remote DataNodes registered.
- `jps` snapshot:
```
1236 ResourceManager
714 NameNode
2924 Jps
959 SecondaryNameNode
```
- HDFS health (`hdfs dfsadmin -report` excerpt) shows 2.74 TB free across three DataNodes with no under‑replicated or missing blocks.
- YARN node list confirms three NodeManagers online:
```
Total Nodes:3
trinh-slave:34607 RUNNING
quynh-slave:40679 RUNNING
quang-slave:39767 RUNNING
```

### Spark & PySpark
- Spark 4.0.0 installed under `/home/dlhnhom2/spark` (`SPARK_HOME` exported); PySpark 4.0.1 installed in the user site-packages.
- Spark configured for standalone mode with Iceberg integration (`spark.conf/spark-defaults.conf`):
```
spark.master spark://khoa-master:7077
spark.sql.catalog.hadoop_catalog org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.hadoop_catalog.type hadoop
spark.sql.catalog.hadoop_catalog.warehouse hdfs://khoa-master:9000/warehouse/iceberg_test
```
- Spark on this environment relies on the remote YARN cluster for execution; the local standalone master/workers are intentionally stopped.
- Spark Thrift Server is running on YARN (`start-thriftserver.sh --master yarn ...`); it currently listens on `0.0.0.0:10000` for PyHive/Superset traffic.
- PySpark shell connects successfully to Spark 4.0.0 (tested via `pyspark --version`).

### Iceberg & HadoopCatalog
- PyIceberg 0.10.0 installed; Spark runtime jars include `iceberg-spark-runtime-4.0_2.13-1.10.0.jar`.
- HadoopCatalog warehouse path configured on HDFS: `hdfs://khoa-master:9000/warehouse/iceberg_test`.
- HDFS path `/warehouse/iceberg_test/default/air_quality` holds the sample Iceberg table created by `create_iceberg_table.py`; extend this namespace for new datasets.
- Ensure Iceberg metadata directory has appropriate permissions and consider version alignment between Spark (4.0.0) and Iceberg runtime (1.10.0).

- `apache-superset` 5.0.0 installed (global site-packages); configuration overrides in `/home/dlhnhom2/superset_venv_config.py` point to SQLite metadata (`superset_venv_metadata.db`).
- Superset process not running (`ps` check empty); restart with `SUPERSET_CONFIG_PATH=/home/dlhnhom2/superset_venv_config.py FLASK_APP=superset.app:create_app venv-superset/bin/superset run -p 8088` when ready for BI demo.
- Current `SECRET_KEY` is a short placeholder (`'dlhnhom2'`); update to a strong random string before production use.
- Dataset creation uses the PyHive URI `hive://dlhnhom2@localhost:10000/default`; keep the Spark Thrift Server alive on port 10000 and sanitize any new ISO timestamps containing `'T'` via `UPDATE ... SET col = REPLACE(col, 'T', ' ')` if they appear in metadata tables.

### Python & Analytics Tooling
- System Python 3.10.12 with pip 22.0.2; key analytics packages already present (`pandas` 2.0.3, `pyarrow` 14.0.2, `requests` 2.32.5).
- Additional utility scripts found in home directory: `create_iceberg_table.py`, `iceberg_test_spark.py`, `add_superset_database.py` (review for workflow automation).

## Gaps & Recommendations
- Restart the host SSH service (requires elevated privileges) to clear zombie `sshd` entries; monitor new SSH usage patterns to prevent defunct processes.
- Install missing base tools (`git`, `node`, `docker`) if required for CI/CD, frontend development, or containerized Superset deployment.
- Automate startup ordering: Hadoop (HDFS/YARN) → Spark jobs (via YARN) → Iceberg table creation scripts → Superset application.
- Rotate the Superset `SECRET_KEY` and re-check `superset.log` after the next launch to ensure datetime parsing remains resolved.
- Keep the Spark Thrift Server process under supervision (YARN app `spark-thriftserver-iceberg`) so Superset/SQL clients retain access to HiveServer2 endpoints.
- Confirm Spark workers on slave nodes and ensure warehouse path permissions align with jobs executed via PySpark.
