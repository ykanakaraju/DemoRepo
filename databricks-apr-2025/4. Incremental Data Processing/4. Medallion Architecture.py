# Databricks notebook source
# MAGIC %fs rm -r /FileStore/databrickscts/checkpoint

# COMMAND ----------

dataset_bookstore = "dbfs:/FileStore/data/bookstore"
spark.conf.set(f"dataset.bookstore", dataset_bookstore)

checkpoint_location = "/FileStore/databrickscts/checkpoint"

# COMMAND ----------

# MAGIC %run ../Includes/copy_utils

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Auto Loader

# COMMAND ----------

(
    spark
    .readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{checkpoint_location}/orders_raw")
    .load(f"{dataset_bookstore}/orders-raw")
    .createOrReplaceTempView("orders_raw_temp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Enriching Raw Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_tmp 
# MAGIC AS (
# MAGIC   SELECT *, current_timestamp() arrival_time, input_file_name() source_file
# MAGIC   FROM orders_raw_temp
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_tmp

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating Bronze Table

# COMMAND ----------

(
      spark.table("orders_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{checkpoint_location}/orders_bronze")
      .outputMode("append")
      .table("orders_bronze")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_bronze

# COMMAND ----------

display(dbutils.fs.ls(f"{dataset_bookstore}/orders-raw"))

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Create a static lookup table for customer data

# COMMAND ----------

(
      spark
      .read
      .format("json")
      .load(f"{dataset_bookstore}/customers-json")
      .createOrReplaceTempView("customers_lookup")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_lookup

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating Silver Table

# COMMAND ----------

(
  spark
  .readStream
  .table("orders_bronze")
  .createOrReplaceTempView("orders_bronze_tmp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW orders_enriched_tmp 
# MAGIC AS 
# MAGIC (
# MAGIC   SELECT 
# MAGIC       order_id, 
# MAGIC       quantity, 
# MAGIC       o.customer_id, 
# MAGIC       c.profile:first_name as f_name, 
# MAGIC       c.profile:last_name as l_name,
# MAGIC       cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, 
# MAGIC       books
# MAGIC   FROM orders_bronze_tmp o
# MAGIC   INNER JOIN customers_lookup c
# MAGIC   ON o.customer_id = c.customer_id
# MAGIC   WHERE quantity > 0
# MAGIC )

# COMMAND ----------

(
      spark
      .table("orders_enriched_tmp")
      .writeStream
      .format("delta")
      .option("checkpointLocation", f"{checkpoint_location}/orders_silver")
      .outputMode("append")
      .table("orders_silver")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_silver

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT COUNT(*) FROM orders_silver

# COMMAND ----------

display(dbutils.fs.ls(f"{dataset_bookstore}/orders-raw"))

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating Gold Table

# COMMAND ----------

(
  spark
  .readStream
  .table("orders_silver")
  .createOrReplaceTempView("orders_silver_tmp")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW daily_customer_books_tmp 
# MAGIC AS (
# MAGIC   SELECT 
# MAGIC       customer_id, 
# MAGIC       f_name, 
# MAGIC       l_name, 
# MAGIC       date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
# MAGIC   FROM 
# MAGIC       orders_silver_tmp
# MAGIC   GROUP BY 
# MAGIC       customer_id, 
# MAGIC       f_name, 
# MAGIC       l_name, 
# MAGIC       date_trunc("DD", order_timestamp)
# MAGIC )

# COMMAND ----------

(
      spark
      .table("daily_customer_books_tmp")
      .writeStream
      .format("delta")
      .outputMode("complete")
      .option("checkpointLocation", f"{checkpoint_location}/daily_customer_books")
      .trigger(availableNow=True)
      .table("daily_customer_books")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM daily_customer_books

# COMMAND ----------

display(dbutils.fs.ls(f"{dataset_bookstore}/orders-raw"))

# COMMAND ----------

load_new_data()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Stopping active streams

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------


