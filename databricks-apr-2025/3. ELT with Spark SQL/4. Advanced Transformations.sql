-- Databricks notebook source
-- MAGIC %python
-- MAGIC dataset_bookstore = "dbfs:/FileStore/data/bookstore"
-- MAGIC spark.conf.set(f"dataset.bookstore", dataset_bookstore)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Parsing JSON Data

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM customers

-- COMMAND ----------

DESCRIBE customers

-- COMMAND ----------

SELECT customer_id, profile:first_name, profile:address:country
FROM customers

-- COMMAND ----------

SELECT 
  customer_id, 
  profile,
  from_json(profile, schema_of_json('{"first_name":"Dniren","last_name":"Abby","gender":"Female","address":{"street":"768 Mesta Terrace","city":"Annecy","country":"France"}}')) AS profile_struct
FROM customers;

-- COMMAND ----------

SELECT profile FROM customers LIMIT 1

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW parsed_customers AS
  SELECT 
    customer_id, 
    from_json(profile, schema_of_json('{"first_name":"Thomas","last_name":"Lane","gender":"Male","address":{"street":"06 Boulevard Victor Hugo","city":"Paris","country":"France"}}')) AS profile_struct
  FROM customers;
  
SELECT * FROM parsed_customers;

-- COMMAND ----------

DESCRIBE parsed_customers

-- COMMAND ----------

SELECT customer_id, profile_struct.first_name, profile_struct.address.*
FROM parsed_customers

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_final AS
  SELECT customer_id, profile_struct.*
  FROM parsed_customers;
  
SELECT * FROM customers_final;

-- COMMAND ----------

SELECT order_id, customer_id, books FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Explode Function

-- COMMAND ----------

SELECT order_id, customer_id, explode(books) AS book FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Collecting Rows

-- COMMAND ----------

SELECT customer_id,
  collect_set(order_id) AS orders_set,
  collect_set(books.book_id) AS books_set
FROM orders
GROUP BY customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Flatten Arrays

-- COMMAND ----------

SELECT customer_id,
  collect_set(books.book_id) As before_flatten,
  array_distinct(flatten(collect_set(books.book_id))) AS after_flatten
FROM orders
GROUP BY customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Join Operations

-- COMMAND ----------

SELECT * FROM books

-- COMMAND ----------

SELECT *, explode(books) AS book FROM orders

-- COMMAND ----------

SELECT *
FROM 
  (SELECT *, explode(books) AS book FROM orders) o
INNER JOIN books b
ON o.book.book_id = b.book_id;

-- COMMAND ----------

CREATE OR REPLACE VIEW orders_enriched AS
SELECT *
FROM 
  (SELECT *, explode(books) AS book FROM orders) o
INNER JOIN books b
ON o.book.book_id = b.book_id;

SELECT * FROM orders_enriched

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Set Operations

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------



-- COMMAND ----------

CREATE TABLE order_updates AS
SELECT * FROM PARQUET.`dbfs:/FileStore/data/bookstore/orders-new/export_004.parquet`

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

SELECT * FROM order_updates

-- COMMAND ----------

SELECT * FROM orders 
UNION 
SELECT * FROM order_updates 

-- COMMAND ----------

SELECT * FROM orders 
INTERSECT 
SELECT * FROM order_updates 

-- COMMAND ----------

SELECT * FROM orders 
MINUS 
SELECT * FROM order_updates 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Reshaping Data with Pivot

-- COMMAND ----------

SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched

-- COMMAND ----------

CREATE OR REPLACE TABLE transactions AS
SELECT * FROM (
  SELECT
    customer_id,
    book.book_id AS book_id,
    book.quantity AS quantity
  FROM orders_enriched
) PIVOT (
  sum(quantity) FOR book_id in (
    'B01', 'B02', 'B03', 'B04', 'B05', 'B06',
    'B07', 'B08', 'B09', 'B10', 'B11', 'B12'
  )
);

SELECT * FROM transactions;

-- COMMAND ----------


