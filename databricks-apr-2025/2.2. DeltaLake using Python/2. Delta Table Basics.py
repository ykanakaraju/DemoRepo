# Databricks notebook source
# MAGIC %md
# MAGIC #### Working with DeltaTable, Delta files and DataFrames
# MAGIC
# MAGIC - Save the DataFrame as delta files and read delta files into a DataFrame
# MAGIC - Update and delete the existing data using delta format

# COMMAND ----------

# MAGIC %md
# MAGIC #### Command Reference
# MAGIC
# MAGIC - Save a DataFrame in delta format files
# MAGIC
# MAGIC   `df.write.format("delta").save(<path>)`
# MAGIC
# MAGIC
# MAGIC - Creating a DataFrame from Delta files
# MAGIC
# MAGIC 	`df = spark.read.format("delta").load(<path>)  `
# MAGIC
# MAGIC
# MAGIC - Create a DeltaTable from Delta files
# MAGIC
# MAGIC 	`from delta.tables import DeltaTable` <br/>
# MAGIC 	`dt = DeltaTable.forPath(spark, <path>)`
# MAGIC
# MAGIC
# MAGIC - Create a DataFrame from a DeltaTable
# MAGIC
# MAGIC 	`df = dt.toDF()`
# MAGIC
# MAGIC
# MAGIC - Creating a SQL table on top of delta format files
# MAGIC
# MAGIC 	`CREATE TABLE db1.students(........)	USING DELTA	LOCATION '<path>'`
# MAGIC
# MAGIC
# MAGIC - Writing a DataFrame into a SQL delta table
# MAGIC
# MAGIC 	`df.write.insertInto("db1.students")`
# MAGIC
# MAGIC
# MAGIC - Create a DeltaTable from SQL delta table
# MAGIC
# MAGIC 	`from delta.tables import DeltaTable` <br/>
# MAGIC 	`dt = DeltaTable.forName(spark, "db1.students")`

# COMMAND ----------

# MAGIC %run "../Includes/utils"

# COMMAND ----------

# MAGIC %md
# MAGIC **Run the previous notebook to create the dataframes from well-formed JSON strings**

# COMMAND ----------

# MAGIC %run "./1. Create Sample Data"

# COMMAND ----------

display(students1_df)

# COMMAND ----------

display(students2_df)

# COMMAND ----------

dataPath = "/FileStore/delta/students"
dbutils.fs.rm(dataPath, True)

# COMMAND ----------

# MAGIC %md
# MAGIC **Save the dataframe into a directory as delta files**

# COMMAND ----------

students1_df.rdd.getNumPartitions()

# COMMAND ----------

students1_df.write.format("delta").save(dataPath)

# COMMAND ----------

print_parquet()

# COMMAND ----------

print_json()

# COMMAND ----------

# MAGIC %md
# MAGIC **Read the delta files into a dataframe**

# COMMAND ----------

students1_df = spark.read.format("delta").load(dataPath)
display(students1_df)

# COMMAND ----------

students1_df.rdd.getNumPartitions()

# COMMAND ----------

# MAGIC %md
# MAGIC **Create a DeltaTable reading from delta file path**

# COMMAND ----------

from delta.tables import DeltaTable

students_delta = DeltaTable.forPath(spark, dataPath)
type(students_delta)

# COMMAND ----------

display( students_delta.toDF() )

# COMMAND ----------

# MAGIC %md 
# MAGIC **Perform an UPDATE using the DeltaTable**

# COMMAND ----------

help(students_delta)

# COMMAND ----------

students_delta.history().display()

# COMMAND ----------

display( students_delta.toDF() )

# COMMAND ----------

students_delta.detail().display()

# COMMAND ----------

help(students_delta.update)

# COMMAND ----------

students_delta.update(
    condition="student_id=4",
    set={"student_email":"'raju@gmail.com'"}
)

# COMMAND ----------

print_parquet()

# COMMAND ----------

students_delta.detail().display()

# COMMAND ----------

print_json()

# COMMAND ----------

# MAGIC %fs head dbfs:/FileStore/delta/students/_delta_log/00000000000000000001.json

# COMMAND ----------

# MAGIC %md 
# MAGIC **Perform a DELETE using the DeltaTable**

# COMMAND ----------

students_delta.delete("student_id=5")

# COMMAND ----------

display(students_delta.toDF())

# COMMAND ----------

# MAGIC %md
# MAGIC **DeltaTable & SQL**

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/delta/students

# COMMAND ----------

students1_str = """
{"students": [{"student_id":1,"student_first_name":"Eduino","student_last_name":"Dawdry","student_email":"edawdry0@whitehouse.gov","student_gender":"Bigender","student_phone_numbers":["5737119029"],"student_address":{"street":"218 Ridgeway Crossing","city":"Omaha","state":"Nebraska","postal_code":"68110"}, "action": "I"},
{"student_id":2,"student_first_name":"Lacee","student_last_name":"Prosek","student_email":"lprosek1@barnesandnoble.com","student_gender":"Polygender","student_phone_numbers":["9526294997","4699651256","7167123799","7061046839","7013761528"],"student_address":{"street":"188 Meadow Vale Avenue","city":"Augusta","state":"Georgia","postal_code":"30919"}, "action": "I"},
{"student_id":3,"student_first_name":"Richart","student_last_name":"Zimmer","student_email":"rzimmer2@ox.ac.uk","student_gender":"Non-binary","student_phone_numbers":["3129072019","2815879465","9793774370","6367833815"],"student_address":{"street":"87155 Lunder Court","city":"Fort Myers","state":"Florida","postal_code":"33994"}, "action": "I"},
{"student_id":4,"student_first_name":"Elyse","student_last_name":"Addionisio","student_email":"","student_gender":"Polygender","student_phone_numbers":["7347984926","3364474838","7136381150"],"student_address":{"street":"77 Sugar Alley","city":"Atlanta","state":"Georgia","postal_code":"31132"}, "action": "I"},
{"student_id":5,"student_first_name":"Lilian","student_last_name":"Warret","student_email":"","student_gender":"Male","student_phone_numbers":["5031246553","6151432197","2152754201"],"student_address":{"street":"82540 Summer Ridge Point","city":"Sioux Falls","state":"South Dakota","postal_code":"57193"}, "action": "I"}
]}
"""

import json
from pyspark.sql import Row

students1 = json.loads(students1_str)
students1_df = spark.createDataFrame( Row(**s) for s in students1["students"])
display(students1_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS students_db;
# MAGIC CREATE DATABASE students_db;
# MAGIC USE students_db;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE IF EXISTS students;
# MAGIC
# MAGIC CREATE TABLE students (
# MAGIC     student_id LONG,
# MAGIC     student_first_name STRING,
# MAGIC     student_last_name STRING,
# MAGIC     student_email STRING,
# MAGIC     student_gender STRING,
# MAGIC     student_phone_numbers ARRAY<STRING>,
# MAGIC     student_address MAP<STRING, STRING>,
# MAGIC     action STRING
# MAGIC ) USING DELTA
# MAGIC LOCATION '/FileStore/delta/students'

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY students

# COMMAND ----------

students1_df.write.insertInto("students")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM students

# COMMAND ----------

# MAGIC %sql
# MAGIC TRUNCATE TABLE students

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM students

# COMMAND ----------

students1_df.createOrReplaceTempView("students1")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO students
# MAGIC SELECT * FROM students1

# COMMAND ----------

# MAGIC %fs ls /FileStore/delta/students

# COMMAND ----------

from delta.tables import DeltaTable

students_delta = DeltaTable.forName(spark, 'students')   # from SQL table in delta format
#students_delta = DeltaTable.forPath(spark, '/FileStore/delta/students')   # from delta file

display(students_delta.toDF())

# COMMAND ----------


