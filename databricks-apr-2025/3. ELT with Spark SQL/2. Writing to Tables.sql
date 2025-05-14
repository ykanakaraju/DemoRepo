-- Databricks notebook source
-- MAGIC %python
-- MAGIC dataset_bookstore = "dbfs:/FileStore/data/bookstore"
-- MAGIC spark.conf.set(f"dataset.bookstore", dataset_bookstore)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Creating Tables

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/data/bookstore/orders

-- COMMAND ----------

SELECT input_file_name() file_name, COUNT(*) as num_rows
FROM PARQUET.`dbfs:/FileStore/data/bookstore/orders`
GROUP BY file_name;

-- COMMAND ----------

SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

CREATE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Overwriting Tables

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESC HISTORY orders

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Appending Data

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

DESC HISTORY orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Merging Data

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS 
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

-- COMMAND ----------

-- DBTITLE 1,TARGET
SELECT * FROM customers

-- COMMAND ----------

-- DBTITLE 1,SOURCE
SELECT * FROM customers_updates

-- COMMAND ----------

MERGE INTO customers AS T 
USING customers_updates AS S 
ON T.customer_id = S.customer_id
WHEN MATCHED AND T.email IS NULL AND S.email IS NOT NULL THEN
  UPDATE SET T.email = S.email, T.updated = S.updated
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

SELECT * FROM customers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **An other example**

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv-new",
  header = "true",
  delimiter = ";"
);

-- COMMAND ----------

SELECT * FROM books_updates

-- COMMAND ----------

SELECT * FROM books

-- COMMAND ----------

MERGE INTO books AS T 
USING books_updates AS S 
ON T.book_id = S.book_id AND T.title = S.title
WHEN NOT MATCHED AND S.category = 'Computer Science' THEN
  INSERT *


-- COMMAND ----------

SELECT * FROM books

-- COMMAND ----------

DESC HISTORY books

-- COMMAND ----------


