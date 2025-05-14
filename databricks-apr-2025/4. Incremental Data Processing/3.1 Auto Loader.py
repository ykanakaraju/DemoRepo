# Databricks notebook source
dataset_bookstore = "dbfs:/FileStore/data/bookstore"
spark.conf.set(f"dataset.bookstore", dataset_bookstore)

checkpoint_location = "/FileStore/databrickscts/checkpoint"

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/databrickscts/checkpoint

# COMMAND ----------

# MAGIC %run ../Includes/copy_utils

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-streaming")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Auto Loader

# COMMAND ----------

orders_df = (
  spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{checkpoint_location}/orders_checkpoint")
    .load(f"{dataset_bookstore}/orders-raw")
)

(
  orders_df
  .writeStream
  .option("checkpointLocation", f"{checkpoint_location}/orders_checkpoint")
  .table("orders_updates")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_updates

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Landing New Files

# COMMAND ----------

load_new_data()

# COMMAND ----------

files = dbutils.fs.ls(f"{dataset_bookstore}/orders-raw")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Let's look at table history

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY orders_updates

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Clean Up

# COMMAND ----------

spark.streams.active[0].stop()

# COMMAND ----------

dbutils.fs.rm(f"{checkpoint_location}/orders_checkpoint", True)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE orders_updates

# COMMAND ----------


