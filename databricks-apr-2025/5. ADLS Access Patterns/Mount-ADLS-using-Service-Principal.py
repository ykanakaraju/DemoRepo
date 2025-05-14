# Databricks notebook source
# MAGIC %md
# MAGIC ### Mount Azure Data Lake using Service Principal
# MAGIC #### Steps to follow
# MAGIC 1. Get client_id, tenant_id and client_secret from key vault
# MAGIC 2. Set Spark Config with App/ Client Id, Directory/ Tenant Id & Secret
# MAGIC 3. Call file system utlity mount to mount the storage
# MAGIC 4. Explore other file system utlities related to mount (list all mounts, unmount)
# MAGIC
# MAGIC - SAS token has an expiry time. Make sure to regenerate the token with READ and LIST access
# MAGIC
# MAGIC Ref: https://learn.microsoft.com/en-us/azure/databricks/dbfs/mounts

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'cts-demo-scope', key = 'client-id')
tenant_id = dbutils.secrets.get(scope = 'cts-demo-scope', key = 'tenant-id')
client_secret_value = dbutils.secrets.get(scope = 'cts-demo-scope', key = 'client-secret-value')

storage_account = "ctsdemosa"
container = "demo"

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret_value,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

# MAGIC %md
# MAGIC **Mount an Azure blob container to Databricks mount point**

# COMMAND ----------

dbutils.fs.mount(
  source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account}/{container}",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls(f"/mnt/{storage_account}/{container}"))

# COMMAND ----------

df = spark.read.csv(f"/mnt/{storage_account}/{container}", header=True)
display(df)

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC **Unmount an existing mount point**

# COMMAND ----------

dbutils.fs.unmount('/mnt/ctsdemosa/demo')
