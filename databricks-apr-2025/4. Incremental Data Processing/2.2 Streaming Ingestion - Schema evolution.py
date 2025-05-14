# Databricks notebook source
# MAGIC %fs rm -r dbfs:/FileStore/checkpoint/invoices

# COMMAND ----------

checkpoint_path = "dbfs:/FileStore/checkpoint/invoices"

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS demodb CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS demodb;
# MAGIC USE demodb;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database()

# COMMAND ----------

# MAGIC %fs rm  dbfs:/FileStore/data/invoices/incremental/invoices_2021.csv

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/data/invoices/incremental

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS invoices (
# MAGIC   InvoiceNo int,
# MAGIC   StockCode string,
# MAGIC   Description string,
# MAGIC   Quantity int,
# MAGIC   InvoiceDate timestamp,
# MAGIC   UnitPrice double,
# MAGIC   CustomerID int
# MAGIC ) 

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Ingest data into the table using a batch query

# COMMAND ----------

invoice_schema = """InvoiceNo int, StockCode string, Description string, Quantity int, 
                    InvoiceDate timestamp, UnitPrice double, CustomerID int"""
                    
source_df = (
  spark.readStream
  .format("csv")
  .option("header", "true")
  .schema(invoice_schema)
  .load("dbfs:/FileStore/data/invoices/incremental")
)

query = (
  source_df.writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_path)
  .outputMode("append")
  .trigger(availableNow = True)
  .toTable("demodb.invoices")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM invoices

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Add more data files with an additional column

# COMMAND ----------

# MAGIC %fs head /FileStore/data/invoices/invoices_2021.csv

# COMMAND ----------

# MAGIC %fs cp /FileStore/data/invoices/invoices_2021.csv /FileStore/data/invoices/incremental

# COMMAND ----------

# MAGIC %fs ls /FileStore/data/invoices/incremental

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM invoices;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Manual schema evalution

# COMMAND ----------

# MAGIC %md
# MAGIC ######Alter table to evolve the schema

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE invoices ADD COLUMNS (Country string)

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Modify streaming ingestion to accomodate schema changes

# COMMAND ----------

invoice_schema = """InvoiceNo int, StockCode string, Description string, Quantity int, 
                    InvoiceDate timestamp, UnitPrice double, CustomerID int, Country string"""

source_df = (
  spark.readStream
  .format("csv")
  .option("header", "true")
  .schema(invoice_schema)
  .load("dbfs:/FileStore/data/invoices/incremental")
)

write_query = (
  source_df.writeStream
  .format("delta")
  .option("checkpointLocation", checkpoint_path)
  .outputMode("append")
  .trigger(availableNow = True)
  .toTable("invoices")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM invoices

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Automatic schema evalution

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/FileStore/checkpoint/invoices

# COMMAND ----------

# MAGIC %fs rm dbfs:/FileStore/data/invoices/incremental/invoices_2021.csv

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/data/invoices/incremental

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS demodb CASCADE;
# MAGIC CREATE DATABASE IF NOT EXISTS demodb;
# MAGIC USE demodb;

# COMMAND ----------

def ingest():

  spark.conf.set("spark.sql.streaming.schemaInference", "true")

  source_df = (
    spark.readStream
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mergeSchema", "true")
    .load("dbfs:/FileStore/data/invoices/incremental")
  )

  write_query = (
    source_df.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
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

# MAGIC %fs cp /FileStore/data/invoices/invoices_2021.csv /FileStore/data/invoices/incremental

# COMMAND ----------

# MAGIC %fs ls /FileStore/data/invoices/incremental

# COMMAND ----------

ingest()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM invoices

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cleanup

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/FileStore/checkpoint/invoices

# COMMAND ----------

# MAGIC %fs rm dbfs:/FileStore/data/invoices/incremental/invoices_2021.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS demodb CASCADE

# COMMAND ----------


