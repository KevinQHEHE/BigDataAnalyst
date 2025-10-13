hdfs dfsadmin -safemode get
# nếu ON (dev/test) thì:
hdfs dfsadmin -safemode leave

MỞ spark-sql INTERACTIVE trên YARN (recommended)

spark-sql --master yarn \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.hadoop_catalog=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.hadoop_catalog.type=hadoop \
  --conf spark.sql.catalog.hadoop_catalog.warehouse=hdfs://khoa-master:9000/warehouse/iceberg

SHOW NAMESPACES IN hadoop_catalog.lh;
DESCRIBE TABLE EXTENDED hadoop_catalog.lh.gold.dim_location;
SELECT COUNT(*) FROM hadoop_catalog.lh.gold.dim_time;
SELECT * FROM hadoop_catalog.lh.gold.dim_time ORDER BY hour;
SELECT * FROM hadoop_catalog.lh.gold.dim_location ORDER BY location_key;
SELECT * FROM hadoop_catalog.lh.gold.dim_pollutant;