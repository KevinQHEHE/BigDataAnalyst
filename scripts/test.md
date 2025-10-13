hdfs dfsadmin -safemode get
hdfs dfsadmin -safemode leave

bash scripts/spark_submit.sh scripts/create_lh_tables.py -- --locations configs/locations.json

Kết nối Spark SQL
SPARK_HOME=${SPARK_HOME:-/home/dlhnhom2/spark}
$SPARK_HOME/bin/spark-sql --master local[1] \
  --conf spark.sql.catalogImplementation=in-memory \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop \
  --conf spark.sql.catalog.hadoop_catalog.warehouse=hdfs://khoa-master:9000/warehouse/iceberg


SHOW NAMESPACES 

DESCRIBE TABLE lh.gold.dim_location
DESCRIBE TABLE lh.gold.dim_time
DESCRIBE TABLE lh.bronze.open_meteo_hourly
DESCRIBE TABLE lh.silver.air_quality_hourly_clean
DESCRIBE TABLE lh.gold.fact_air_quality_hourly
DESCRIBE TABLE lh.gold.fact_city_daily
DESCRIBE TABLE lh.gold.fact_episode