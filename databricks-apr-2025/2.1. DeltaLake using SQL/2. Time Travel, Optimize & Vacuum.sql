-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC #### Time Travel 
-- MAGIC **Time Travel feature allows us to rollback to a specific version of the table**

-- COMMAND ----------

DESC HISTORY users


-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

-- SELECT * FROM users VERSION AS OF 4
SELECT * FROM users@v2

-- COMMAND ----------

DELETE FROM users

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users/_delta_log/

-- COMMAND ----------

-- MAGIC %fs head dbfs:/user/hive/warehouse/users/_delta_log/00000000000000000006.json

-- COMMAND ----------

DESC DETAIL users

-- COMMAND ----------

DESC HISTORY users


-- COMMAND ----------

SELECT * FROM users@v5

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

RESTORE TABLE users TO VERSION AS OF 4

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users/_delta_log/

-- COMMAND ----------

-- MAGIC %fs head dbfs:/user/hive/warehouse/users/_delta_log/00000000000000000007.json

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

DELETE FROM users WHERE age > 25

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

DESC DETAIL users

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

RESTORE TABLE users TO TIMESTAMP AS OF '2025-04-23 04:30:00'

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users

-- COMMAND ----------

DESC DETAIL users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### OPTIMIZE Command
-- MAGIC **The OPTIMIZE command rewrites data files to improve data layout for Delta tables.**

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Z Ordering**
-- MAGIC
-- MAGIC Z Ordering your data reorganizes the data in storage and allows certain queries to read less data, so they run faster. When your data is appropriately ordered, more files can be skipped.
-- MAGIC
-- MAGIC Z Order is particularly important for the ordering of multiple columns. If you only need to order by a single column, then simple sorting suffices. If there are multiple columns, but we always/only query a common prefix of those columns, then hierarchical sorting suffices. Z Ordering is good when querying on one or multiple columns. 

-- COMMAND ----------

DESCRIBE DETAIL users

-- COMMAND ----------

OPTIMIZE users ZORDER BY (gender, id)


-- COMMAND ----------

DESCRIBE DETAIL users

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users

-- COMMAND ----------

DESC DETAIL users

-- COMMAND ----------

OPTIMIZE users 

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

DESC DETAIL users

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### VACUUM Command

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - VACUUM removes all files from the table directory that are not managed by Delta, as well as data files that are no longer in the latest state of the transaction log for the table and are older than a retention threshold. 
-- MAGIC - VACUUM will skip all directories that begin with an underscore (_), which includes the _delta_log. Partitioning your table on a column that begins with an underscore is an exception to this rule; 
-- MAGIC - VACUUM scans all valid partitions included in the target Delta table. 
-- MAGIC - Delta table data files are deleted according to the time they have been logically removed from Delta’s transaction log plus retention hours, not their modification timestamps on the storage system. The default threshold is 7 days.

-- COMMAND ----------

DESC DETAIL users

-- COMMAND ----------

VACUUM users

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false

-- COMMAND ----------

VACUUM users RETAIN 1 HOURS

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users

-- COMMAND ----------

VACUUM users RETAIN 0 HOURS

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users

-- COMMAND ----------

DESC HISTORY users

-- COMMAND ----------

RESTORE TABLE users TO VERSION AS OF 8

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Drop the table

-- COMMAND ----------

DROP TABLE users

-- COMMAND ----------

SELECT * FROM users

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/users

-- COMMAND ----------


