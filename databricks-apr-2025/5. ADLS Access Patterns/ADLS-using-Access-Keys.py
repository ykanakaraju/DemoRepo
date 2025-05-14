# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using access keys
# MAGIC 1. Set the spark config `fs.azure.account.key`
# MAGIC 1. List files from `demo` container
# MAGIC 1. Read data from `circuits.csv` file

# COMMAND ----------

storage_account = "ctsdemosa"
container_name = "demo"
file_name = "circuits.csv"
access_key = "<YOUR_ACCESS_KEY>"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.key.{storage_account}.dfs.core.windows.net", access_key)

# COMMAND ----------

display(dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net"))

# COMMAND ----------

file_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{file_name}"
df = spark.read.csv(file_path, header=True)
display(df)

# COMMAND ----------


