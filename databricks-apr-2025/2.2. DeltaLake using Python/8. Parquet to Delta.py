# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/data/bookstore/orders/

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/output

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create some sample parquet datasets

# COMMAND ----------

df1 = (
    spark
    .read
    .parquet("dbfs:/FileStore/data/bookstore/orders/")
    .withColumn("order_date", to_date(to_timestamp("order_timestamp"), 'y-M-d'))
)

df1.write.parquet("/FileStore/output/parquet1")
df1.write.partitionBy("order_date").parquet("/FileStore/output/parquet2")
df1.write.partitionBy("order_date").parquet("/FileStore/output/parquet3")

# COMMAND ----------

df1.printSchema()

# COMMAND ----------

# MAGIC %fs ls /FileStore/output/parquet1

# COMMAND ----------

# MAGIC %md
# MAGIC #### Converting Parquet Dataset to Delta using Python

# COMMAND ----------

from delta.tables import *

# COMMAND ----------


deltaTable1 = DeltaTable.convertToDelta(spark, "parquet.`/FileStore/output/parquet1`")


# COMMAND ----------

# MAGIC %fs ls /FileStore/output/parquet1

# COMMAND ----------

deltaTable.history().display()

# COMMAND ----------

# MAGIC %fs ls /FileStore/output/parquet2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Converting Partitioned Parquet Dataset to Delta using Python

# COMMAND ----------

# converting partitioned parquet dataset to delta format
deltaTable2 = DeltaTable.convertToDelta(spark, "parquet.`/FileStore/output/parquet2`", "order_date DATE")

# COMMAND ----------

deltaTable2.history().display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Converting Partitioned Parquet Dataset to Delta using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA PARQUET.`/FileStore/output/parquet3`
# MAGIC PARTITIONED BY (order_date DATE)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DELTA.`/FileStore/output/parquet3`

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cleanup

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/output

# COMMAND ----------


