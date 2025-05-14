# Databricks notebook source
file_path = "dbfs:/FileStore/data"

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

for f in dbutils.fs.ls(file_path):
    if ".json" in f.name:
        print(f.path)

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/users.json")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore")

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/demo1")

# COMMAND ----------

dbutils.fs.mkdirs("dbfs:/FileStore/demo2")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/university/department.csv", "dbfs:/FileStore/demo1")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/university", "dbfs:/FileStore/demo1", recurse=True)

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/demo1")

# COMMAND ----------

dbutils.fs.mv("dbfs:/FileStore/demo1/department.csv", "dbfs:/FileStore/demo2")

# COMMAND ----------

dbutils.fs.ls("dbfs:/FileStore/demo2")

# COMMAND ----------

dbutils.fs.mv("dbfs:/FileStore/university", "dbfs:/FileStore/demo2/university", True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/demo2/department.csv")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/demo2/university/", True)

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/demo2")

# COMMAND ----------

dbutils.fs.rm("dbfs:/FileStore/demo1", True)

# COMMAND ----------

print(dbutils.fs.head("dbfs:/FileStore/wordcount.txt"))

# COMMAND ----------


