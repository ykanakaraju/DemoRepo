# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ####vacuum - Remove files no longer referenced by a Delta table####
# MAGIC
# MAGIC You can remove files no longer referenced by a Delta table and are older than the retention threshold by running the ***vacuum*** command on the table. 
# MAGIC
# MAGIC vacuum is not triggered automatically. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC * [Delta Table utility functions ducumentation](https://docs.delta.io/latest/delta-utility.html#remove-files-no-longer-referenced-by-a-delta-table) 

# COMMAND ----------

# MAGIC %run "./1. Create Sample Data"

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Vaccuum using DeltaTable object

# COMMAND ----------

students_delta.vacuum(0)

# COMMAND ----------

# MAGIC %md 
# MAGIC Equivalent Vaccuum related commands using SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- VACUUM students_db.students
# MAGIC -- VACUUM students_db.students RETAIN 0 HOURS 
# MAGIC -- DESCRIBE HISTORY students_db.students

# COMMAND ----------

spark.conf.get('spark.databricks.delta.retentionDurationCheck.enabled')

# COMMAND ----------

spark.conf.set('spark.databricks.delta.retentionDurationCheck.enabled', 'false')

# COMMAND ----------

students_delta.vacuum(0)

# COMMAND ----------

display(students_delta.toDF())

# COMMAND ----------

display(students_delta.history())

# COMMAND ----------

students_delta.restoreToVersion(6)

# COMMAND ----------

# MAGIC %fs ls /FileStore/delta/students

# COMMAND ----------

# MAGIC %fs ls /FileStore/delta/students/_delta_log

# COMMAND ----------


