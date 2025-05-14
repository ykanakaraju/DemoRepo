-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### `COPY INTO` command
-- MAGIC
-- MAGIC [Documentation](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into)

-- COMMAND ----------

DROP DATABASE IF EXISTS demodb;
CREATE DATABASE IF NOT EXISTS demodb;
USE demodb;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/data/invoices/incremental

-- COMMAND ----------

-- MAGIC %fs head dbfs:/FileStore/data/invoices/incremental/invoices_01_06_2022.csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create a schemaless delta table to ingest invoices data

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS invoices;

-- COMMAND ----------

DESCRIBE EXTENDED invoices;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Ingest data using `COPY INTO` command

-- COMMAND ----------

COPY INTO invoices
FROM "/FileStore/data/invoices/incremental" 
FILEFORMAT = CSV
FORMAT_OPTIONS 
(
  'header' = 'true', 
  'inferSchema' = 'true', 
  'timestampFormat' = 'd-M-y H.m', 
  'mergeSchema' = 'true'
)
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

SELECT * FROM invoices

-- COMMAND ----------

DESCRIBE HISTORY invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Add more data files with an additional column

-- COMMAND ----------

-- MAGIC %fs cp /FileStore/data/invoices/invoices_2021.csv /FileStore/data/invoices/incremental

-- COMMAND ----------

-- MAGIC %fs ls /FileStore/data/invoices/incremental

-- COMMAND ----------

-- MAGIC %fs head dbfs:/FileStore/data/invoices/incremental/invoices_2021.csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Ingest data into the table again

-- COMMAND ----------

COPY INTO invoices
FROM "/FileStore/data/invoices/incremental" 
FILEFORMAT = CSV
FORMAT_OPTIONS 
(
  'header' = 'true', 
  'inferSchema' = 'true', 
  'timestampFormat' = 'd-M-y H.m', 
  'mergeSchema' = 'true'
)
COPY_OPTIONS ('mergeSchema' = 'true')

-- COMMAND ----------

SELECT * FROM invoices

-- COMMAND ----------

DESCRIBE invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Cleanup

-- COMMAND ----------

DROP DATABASE IF EXISTS demodb CASCADE

-- COMMAND ----------

-- MAGIC %fs rm dbfs:/FileStore/data/invoices/incremental/invoices_2021.csv

-- COMMAND ----------

-- MAGIC %fs ls /FileStore/data/invoices/incremental

-- COMMAND ----------


