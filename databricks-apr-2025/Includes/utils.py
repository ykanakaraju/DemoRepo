# Databricks notebook source
def reset_bookstore_dataset():
    dbutils.fs.rm("dbfs:/FileStore/data/bookstore", True)
    dbutils.fs.cp("dbfs:/FileStore/data/bookstore_original", "dbfs:/FileStore/data/bookstore", True)

# COMMAND ----------

def print_files(path) :
    i = 0
    for f in dbutils.fs.ls(path):
        if ".parquet" in f.path:
            print(f.path)
            i = i+1
    print(f"\n number of parquet files = {i}")
    pass

# COMMAND ----------

def print_logs(path) :
    i = 0
    for f in dbutils.fs.ls(f"{path}/_delta_log"):
        if ".json" in f.path:
            print(f.path)
            i = i+1
    print(f"\n number of json files = {i}")
    pass

# COMMAND ----------

def print_parquet():
    print_files("/FileStore/delta/students")
    pass

# COMMAND ----------

def print_json():
    print_logs("/FileStore/delta/students")
    pass

# COMMAND ----------


