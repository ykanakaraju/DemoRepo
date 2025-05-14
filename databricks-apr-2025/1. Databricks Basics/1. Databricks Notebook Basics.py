# Databricks notebook source
# MAGIC %md
# MAGIC ####Running default code

# COMMAND ----------

course = "Databricks"
print(f"Course: {course}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Running SQL code

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_database()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Running Scala code

# COMMAND ----------

# MAGIC %scala
# MAGIC val course = "Databricks"
# MAGIC println(s"Course : $course")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Running an external notebook - **%run** command

# COMMAND ----------

# MAGIC %run "./sample-data"

# COMMAND ----------

name

# COMMAND ----------

# MAGIC %md
# MAGIC ####DBFS File Operations -` %fs`

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

# MAGIC %fs help cp

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/demo1

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/demo2

# COMMAND ----------

# MAGIC %fs rm dbfs:/FileStore/demo2/

# COMMAND ----------

# MAGIC %fs rm dbfs:/FileStore/users.txt

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/FileStore/checkpoint/

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/university/

# COMMAND ----------

# MAGIC %fs cp -r dbfs:/FileStore/university/ dbfs:/FileStore/demo1

# COMMAND ----------

# MAGIC %fs cp -r dbfs:/FileStore/university/ dbfs:/FileStore/demo1/university

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/demo1

# COMMAND ----------

# MAGIC %fs mv dbfs:/FileStore/demo1/department.csv dbfs:/FileStore/demo2

# COMMAND ----------

# MAGIC %fs mv -r dbfs:/FileStore/demo1/university/ dbfs:/FileStore/demo2/university

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/demo2

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/demo1/users.txt

# COMMAND ----------

# MAGIC %fs ls dbfs:/FileStore/demo1

# COMMAND ----------

# MAGIC %fs rm -r dbfs:/FileStore/university

# COMMAND ----------


