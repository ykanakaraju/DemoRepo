# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using SAS Token
# MAGIC 1. Set the spark config for SAS Token
# MAGIC 1. List files from demo container
# MAGIC 1. Read data from circuits.csv file
# MAGIC
# MAGIC - SAS token has an expiry time. Make sure to regenerate the token with READ and LIST access

# COMMAND ----------

storage_account = "ctsdemosa"
container_name = "demo"
file_name = "circuits.csv"
sas_token = "<YOUR_SAS_TOKEN>"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "SAS")
spark.conf.set(f"fs.azure.sas.token.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set(f"fs.azure.sas.fixed.token.{storage_account}.dfs.core.windows.net", sas_token)

# COMMAND ----------

display(dbutils.fs.ls(f"abfss://{container_name}@{storage_account}.dfs.core.windows.net"))

# COMMAND ----------

file_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/{file_name}"
df = spark.read.csv(file_path, header=True)
display(df)

# COMMAND ----------


