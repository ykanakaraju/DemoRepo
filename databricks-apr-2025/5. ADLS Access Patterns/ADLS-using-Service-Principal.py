# Databricks notebook source
# MAGIC %md
# MAGIC #### Access Azure Data Lake using Service Principal
# MAGIC 1. Register Azure AD Application / Service Principal
# MAGIC 1. Generate a secret/ password for the Application
# MAGIC 1. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 1. Assign Role 'Storage Blob Data Contributor' to the Data Lake.
# MAGIC
# MAGIC - URL: https://learn.microsoft.com/en-us/azure/databricks/storage/azure-storage

# COMMAND ----------

client_id = "<CLIENT_ID>"
tenant_id = "<TENENT_ID>"
client_secret_value = "<CLIENT_SECRET_VALUE>"
storage_account = "ctsdemosa"

# COMMAND ----------

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret_value)
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@ctsdemosa.dfs.core.windows.net"))

# COMMAND ----------

df = spark.read.csv("abfss://demo@ctsdemosa.dfs.core.windows.net/circuits.csv", header=True)
display(df)

# COMMAND ----------


