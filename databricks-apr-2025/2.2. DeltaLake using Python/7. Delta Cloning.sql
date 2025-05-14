-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Delta Table Cloning
-- MAGIC
-- MAGIC - You can create a copy of an existing Delta Lake table on Databricks at a specific version using the **`clone`** command. 
-- MAGIC - Clones can be either **deep** or **shallow**.
-- MAGIC
-- MAGIC - A **deep clone** is a clone that **copies the source table data** to the clone target in addition to the metadata of the existing table.
-- MAGIC
-- MAGIC - A **shallow clone** is a clone that **does not copy the data files** to the clone target. The table metadata is equivalent to the source. These clones are cheaper to create.
-- MAGIC
-- MAGIC - Any changes made to either deep or shallow clones affect only the clones themselves and not the source table.
-- MAGIC
-- MAGIC - A cloned table has an independent history from its source table. Time travel queries on a cloned table do not work with the same inputs as they work on its source table.
-- MAGIC

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS demodb;

USE demodb;

-- COMMAND ----------

SELECT current_schema()

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS users_managed 
(id INT, name STRING, age INT, gender STRING);

-- COMMAND ----------

INSERT INTO users_managed 
VALUES 
(1, "Raju", 48, "Male"), (2, "Ramesh", 25, "Male"),
(3, "Ramya", 38, "Female"), (4, "Radhe", 45, "Female"),
(5, "Ravi", 28, "Male"), (6, "Raheem", 25, "Male"),
(7, "Revati", 40, "Female"), (8, "Raghu", 35, "Male");

-- COMMAND ----------

DESCRIBE EXTENDED users_managed

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/demodb.db

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/demodb.db/users_managed

-- COMMAND ----------

SELECT * FROM users_managed

-- COMMAND ----------

UPDATE users_managed 
SET age = age - 1
WHERE gender = 'Male'

-- COMMAND ----------

DELETE FROM  users_managed WHERE id = 8

-- COMMAND ----------

SELECT * FROM users_managed

-- COMMAND ----------

DESCRIBE HISTORY users_managed

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/demodb.db/users_managed/_delta_log/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Deep Clone
-- MAGIC
-- MAGIC - Deep clones do not depend on the source from which they were cloned, but are expensive to create because a deep clone copies the data as well as the metadata.
-- MAGIC
-- MAGIC #### Shallow Clone
-- MAGIC
-- MAGIC - Shallow clones reference data files in the source directory. If you run vacuum on the source table, clients can no longer read the referenced data files and a FileNotFoundException is thrown. 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC from delta.tables import *
-- MAGIC
-- MAGIC dt_users_managed = DeltaTable.forName(spark, "users_managed")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # clone the source at latest version
-- MAGIC dt_users_managed.clone (
-- MAGIC   target="users_cloned", 
-- MAGIC   isShallow=False, 
-- MAGIC   replace=False
-- MAGIC ) 

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM users_cloned

-- COMMAND ----------

DESCRIBE EXTENDED users_cloned

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/demodb.db/users_cloned

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/demodb.db/users_cloned/_delta_log/

-- COMMAND ----------

DESCRIBE HISTORY users_cloned

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC dt_users_managed.clone (
-- MAGIC   target="users_cloned_shallow", 
-- MAGIC   isShallow=True, 
-- MAGIC   replace=False
-- MAGIC ) 

-- COMMAND ----------

SELECT * FROM users_cloned_shallow

-- COMMAND ----------

DESC EXTENDED users_cloned_shallow

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/demodb.db/users_cloned_shallow

-- COMMAND ----------

DESCRIBE HISTORY users_cloned_shallow

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # clone the source at a specific version
-- MAGIC dt_users_managed.cloneAtVersion(
-- MAGIC     version=1, 
-- MAGIC     target="users_cloned_shallow_v1", 
-- MAGIC     isShallow=True, 
-- MAGIC     replace=False
-- MAGIC ) 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # clone the source at a specific version
-- MAGIC dt_users_managed.cloneAtTimestamp(
-- MAGIC     timestamp="2024-10-10 12:41:28", 
-- MAGIC     target="users_cloned_ts", 
-- MAGIC     isShallow=False, 
-- MAGIC     replace=False
-- MAGIC ) 

-- COMMAND ----------

DESCRIBE EXTENDED users_cloned_ts

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/demodb.db/users_cloned_ts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Cleanup

-- COMMAND ----------

DROP SCHEMA demodb CASCADE

-- COMMAND ----------


