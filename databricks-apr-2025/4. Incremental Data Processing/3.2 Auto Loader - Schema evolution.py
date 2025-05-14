# Databricks notebook source
# MAGIC %md
# MAGIC ### Documentation
# MAGIC
# MAGIC [Auto Loader Documentation](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/)
# MAGIC
# MAGIC [Auto Loader options](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS demodb CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS demodb;
# MAGIC USE demodb;

# COMMAND ----------

checkpoint_dir = "/FileStore/checkpoint/invoices_schema"
dbutils.fs.rm(checkpoint_dir, True)

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/data/invoices/incremental

# COMMAND ----------

# MAGIC %fs rm dbfs:/FileStore/data/invoices/incremental/invoices_2022.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM CSV.`dbfs:/FileStore/data/invoices/incremental/invoices_01_06_2022.csv`

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Ingest data

# COMMAND ----------

def ingest():

  source_df = (
    spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")  
    .option("header", "true") 
    .option("timestampFormat","d-M-y H.m")                  
    .option("cloudFiles.schemaLocation", checkpoint_dir)
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaHints", "InvoiceNo string, CustomerID string")
    .load("dbfs:/FileStore/data/invoices/incremental")
  )

  query = (
    source_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_dir)
    .option("mergeSchema", "true")
    .outputMode("append")                          
    .trigger(availableNow = True)
    .toTable("demodb.invoices")
  )

# COMMAND ----------

ingest() 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM invoices

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE invoices

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM invoices WHERE _rescued_data IS NOT NULL

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Add some data files with an additional column

# COMMAND ----------

# MAGIC %fs head /FileStore/data/invoices/invoices_2021.csv

# COMMAND ----------

# MAGIC %fs cp /FileStore/data/invoices/invoices_2021.csv /FileStore/data/invoices/incremental

# COMMAND ----------

# MAGIC %fs ls /FileStore/data/invoices/incremental

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ###### ingest with a retry. 
# MAGIC - This may not work first time. Try again

# COMMAND ----------

ingest()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM invoices

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE invoices

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Ingest some more records with potential bad records

# COMMAND ----------

# MAGIC %fs head /FileStore/data/invoices/invoices_2022.csv

# COMMAND ----------

# MAGIC %fs cp /FileStore/data/invoices/invoices_2022.csv /FileStore/data/invoices/incremental

# COMMAND ----------

ingest()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Check the rescued data

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM invoices where _rescued_data is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY invoices

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE invoices

# COMMAND ----------


