# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using cluster scoped credentials
# MAGIC 1. Set the spark config `fs.azure.account.key` in the cluster
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file

# COMMAND ----------

storage_account = "ctsdemosa"
container_name = "demo"
file_name = "circuits.csv"

# COMMAND ----------

display(dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net"))

# COMMAND ----------

file_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{file_name}"
df = spark.read.csv(file_path, header=True)
display(df)

# COMMAND ----------


