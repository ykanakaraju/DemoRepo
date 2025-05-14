# Databricks notebook source
dbutils.notebook.help()

# COMMAND ----------

# DBTITLE 1,Not supported in community edition
response = dbutils.notebook.run(
    "./3. Databricks Utilities - dbutils.widgets",
    20,
    {"name":"Raju", "city":"Pune"}
)

# COMMAND ----------


