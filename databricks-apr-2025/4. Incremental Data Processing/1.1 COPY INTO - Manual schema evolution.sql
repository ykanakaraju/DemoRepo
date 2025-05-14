-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### `COPY INTO` command
-- MAGIC
-- MAGIC [Documentation](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into)

-- COMMAND ----------

DROP DATABASE IF EXISTS demodb CASCADE;
CREATE DATABASE IF NOT EXISTS demodb;
USE demodb;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/data/invoices/incremental/

-- COMMAND ----------

SELECT * FROM CSV.`dbfs:/FileStore/data/invoices/incremental/invoices_01_06_2022.csv`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create a delta table to ingest invoices data

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS invoices (
  InvoiceNo STRING,
  StockCode STRING,
  Description STRING,
  Quantity INT,
  InvoiceDate TIMESTAMP,
  UnitPrice DOUBLE,
  CustomerID STRING
) USING DELTA

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESC EXTENDED invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### `COPY INTO` command for data ingestion

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/data/invoices/incremental

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Ingest using `COPY INTO` command

-- COMMAND ----------

COPY INTO invoices
FROM 
(
  SELECT 
    InvoiceNo::string, 
    StockCode::string, 
    Description::string, 
    Quantity::int,
    to_timestamp(InvoiceDate,'d-M-y H.m') InvoiceDate, 
    UnitPrice::double, 
    CustomerID::string
  FROM "/FileStore/data/invoices/incremental"
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')

-- COMMAND ----------

SELECT COUNT(*) FROM invoices

-- COMMAND ----------

DESC HISTORY invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### `COPY INTO` is idempotent

-- COMMAND ----------

-- MAGIC %fs cp /FileStore/data/invoices/invoices_2021.csv /FileStore/data/invoices/incremental

-- COMMAND ----------

-- MAGIC %fs ls /FileStore/data/invoices/incremental

-- COMMAND ----------

SELECT * FROM CSV.`dbfs:/FileStore/data/invoices/incremental/invoices_2021.csv`

-- COMMAND ----------

COPY INTO invoices
FROM 
(
  SELECT 
    InvoiceNo::string, 
    StockCode::string, 
    Description::string, 
    Quantity::int,
    to_timestamp(InvoiceDate,'d-M-y H.m') InvoiceDate, 
    UnitPrice::double, 
    CustomerID::string
  FROM "/FileStore/data/invoices/incremental"
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')

-- COMMAND ----------

SELECT COUNT(*) FROM invoices

-- COMMAND ----------

SELECT * FROM invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###### Add more data files with an additional column

-- COMMAND ----------

-- MAGIC %fs cp /FileStore/data/invoices/invoices_2022.csv /FileStore/data/invoices/incremental

-- COMMAND ----------

-- MAGIC %fs ls /FileStore/data/invoices/incremental

-- COMMAND ----------

-- MAGIC %fs head dbfs:/FileStore/data/invoices/incremental/invoices_2022.csv

-- COMMAND ----------

COPY INTO invoices
FROM 
(
  SELECT 
    InvoiceNo::string, 
    StockCode::string, 
    Description::string, 
    Quantity::int,
    to_timestamp(InvoiceDate,'d-M-y H.m') InvoiceDate, 
    UnitPrice::double, 
    CustomerID::string
  FROM "/FileStore/data/invoices/incremental"
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true')

-- COMMAND ----------

REFRESH TABLE invoices

-- COMMAND ----------

SELECT * FROM invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Alter table to manully accomodate the additional field

-- COMMAND ----------

ALTER TABLE invoices ADD COLUMNS (Country string)

-- COMMAND ----------

DESC invoices

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ######Let's add code to manually accomodate the additional field

-- COMMAND ----------

COPY INTO invoices
FROM 
(
  SELECT 
    InvoiceNo::string, 
    StockCode::string, 
    Description::string, 
    Quantity::int,
    to_timestamp(InvoiceDate,'d-M-y H.m') InvoiceDate, 
    UnitPrice::double, 
    CustomerID::string, 
    Country::string
  FROM "/FileStore/data/invoices/incremental"
)
FILEFORMAT = CSV
FORMAT_OPTIONS ('header' = 'true', 'mergeSchema' = 'true')

-- COMMAND ----------

SELECT * FROM invoices

-- COMMAND ----------

-- MAGIC %fs cp /FileStore/data/invoices/invoices_2022.csv /FileStore/data/invoices/incremental

-- COMMAND ----------

-- MAGIC %fs ls /FileStore/data/invoices/incremental

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Cleanup

-- COMMAND ----------

-- MAGIC %fs rm dbfs:/FileStore/data/invoices/incremental/invoices_2021.csv

-- COMMAND ----------

DROP TABLE invoices

-- COMMAND ----------

DROP DATABASE IF EXISTS demodb CASCADE

-- COMMAND ----------


