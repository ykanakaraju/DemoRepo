-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Dataset relations**
-- MAGIC 1. customers.customer_id = orders.customer_id
-- MAGIC 1. orders.books = books.book_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Querying JSON 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dataset_bookstore = "dbfs:/FileStore/data/bookstore"
-- MAGIC spark.conf.set(f"dataset.bookstore", dataset_bookstore)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC files = dbutils.fs.ls(f"{dataset_bookstore}/customers-json")
-- MAGIC display(files)

-- COMMAND ----------

SELECT * FROM JSON.`dbfs:/FileStore/data/bookstore/customers-json/export_001.json`

-- COMMAND ----------

SELECT customer_id, email, profile:first_name, profile:last_name, profile:gender, profile:address:country
FROM JSON.`dbfs:/FileStore/data/bookstore/customers-json/export_001.json`
WHERE email IS NOT NULL AND profile:gender = "Male"

-- COMMAND ----------

-- SELECT * FROM JSON.`dbfs:/FileStore/data/bookstore/customers-json/export_*.json`
SELECT * FROM JSON.`dbfs:/FileStore/data/bookstore/customers-json`

-- COMMAND ----------

SELECT input_file_name() file_name, COUNT(*) as num_rows 
FROM JSON.`dbfs:/FileStore/data/bookstore/customers-json`
GROUP BY file_name

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Querying text format

-- COMMAND ----------

SELECT * FROM TEXT.`dbfs:/FileStore/data/bookstore/customers-json/export_002.json`

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #### Querying binaryFile Format

-- COMMAND ----------

--SELECT * FROM binaryfile.`dbfs:/FileStore/data/bookstore/customers-json/export_002.json`
SELECT * FROM binaryfile.`dbfs:/FileStore/data/bookstore/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Querying CSV 

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/data/bookstore/books-csv

-- COMMAND ----------

-- MAGIC %fs head dbfs:/FileStore/data/bookstore/books-csv/export_001.csv

-- COMMAND ----------

SELECT * FROM CSV.`dbfs:/FileStore/data/bookstore/books-csv/export_001.csv`

-- COMMAND ----------

DROP TABLE books_csv;

-- COMMAND ----------

CREATE TABLE books_csv (
  book_id STRING,
  title STRING,
  author STRING,
  category STRING,
  price DOUBLE
)
USING CSV
OPTIONS( header="true", delimiter=";")
LOCATION 'dbfs:/FileStore/data/bookstore/books-csv'

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESC EXTENDED books_csv

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Limitations of Non-Delta Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - DML operations such as UPDATE, DELETE, MERGE are not supported in Non-Delta formats.
-- MAGIC - INSERT is allowed. 

-- COMMAND ----------

INSERT INTO books_csv(book_id, title) VALUES('B9999', 'Some title')

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

UPDATE books_csv SET price = price + 1

-- COMMAND ----------

DELETE FROM books_csv WHERE price = 22

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/FileStore/data/bookstore/books-csv

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df_books_csv = spark.table("books_csv")
-- MAGIC df_books_csv.write.csv("dbfs:/FileStore/data/bookstore/books-csv", header=True, sep=";", mode="append")

-- COMMAND ----------

REFRESH TABLE books_csv

-- COMMAND ----------

SELECT * FROM books_csv

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### CTAS Statements

-- COMMAND ----------

CREATE TABLE customers AS
SELECT * FROM JSON.`dbfs:/FileStore/data/bookstore/customers-json`

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESC EXTENDED customers

-- COMMAND ----------

CREATE TEMP VIEW books_csv_tmp_vw (
  book_id STRING,
  title STRING,
  author STRING,
  category STRING,
  price DOUBLE
)
USING CSV
OPTIONS( 
  path = "dbfs:/FileStore/data/bookstore/books-csv",
  header="true", 
  delimiter=";"
)


-- COMMAND ----------

SELECT * FROM books_csv_tmp_vw

-- COMMAND ----------

CREATE TABLE books AS 
SELECT * FROM books_csv_tmp_vw;

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------


