-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Creating Delta Lake Table

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

DESC DATABASE EXTENDED default

-- COMMAND ----------

CREATE TABLE users (id INT, name STRING, age INT, gender STRING);

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DESC EXTENDED users

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users/_delta_log/

-- COMMAND ----------

-- MAGIC %fs head dbfs:/user/hive/warehouse/users/_delta_log/00000000000000000000.json

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Insert rows in the table

-- COMMAND ----------

-- Every insert command runs as a transaction and creates a data file
INSERT INTO users VALUES (1, "Raju", 48, "Male"), (2, "Ramesh", 25, "Male");
INSERT INTO users VALUES (3, "Ramya", 38, "Female"), (4, "Radhe", 45, "Female");
INSERT INTO users VALUES (5, "Ravi", 28, "Male"), (6, "Raheem", 25, "Male");
INSERT INTO users VALUES (7, "Revati", 40, "Female"), (8, "Raghu", 35, "Male");

-- Only last statement's result appear as output

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Let's describe the table to see its metadata**

-- COMMAND ----------

DESC EXTENDED users

-- COMMAND ----------

DESC DETAIL users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Lets check the table directory to see what files are created

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users/_delta_log/

-- COMMAND ----------

-- MAGIC %fs head dbfs:/user/hive/warehouse/users/_delta_log/00000000000000000001.json

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Let's do an update operation and see what happens

-- COMMAND ----------

UPDATE users SET age = age + 1 WHERE gender = "Male"

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users

-- COMMAND ----------

-- MAGIC %fs head dbfs:/user/hive/warehouse/users/_delta_log/00000000000000000005.json

-- COMMAND ----------

DESC DETAIL users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Let's check table history

-- COMMAND ----------

DESCRIBE HISTORY users

-- COMMAND ----------

SELECT * FROM users VERSION AS OF 5

-- COMMAND ----------



-- COMMAND ----------


