-- Databricks notebook source
-- MAGIC %md
-- MAGIC **_This is continuation of previous notebook _**

-- COMMAND ----------

USE students_db

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

DROP TABLE students_db.students

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/FileStore/delta/students

-- COMMAND ----------

CREATE TABLE students_db.students (
  student_id INT,
  student_first_name STRING,
  student_last_name STRING,
  student_email STRING,
  student_gender STRING,
  student_phone_numbers ARRAY<STRING>,
  student_address MAP<STRING, STRING>,
  action STRING
) USING DELTA
LOCATION '/FileStore/delta/students'

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC students1_str = """
-- MAGIC {"students": [{"student_id":1,"student_first_name":"Eduino","student_last_name":"Dawdry","student_email":"edawdry0@whitehouse.gov","student_gender":"Bigender","student_phone_numbers":["5737119029"],"student_address":{"street":"218 Ridgeway Crossing","city":"Omaha","state":"Nebraska","postal_code":"68110"}, "action": "I"},
-- MAGIC {"student_id":2,"student_first_name":"Lacee","student_last_name":"Prosek","student_email":"lprosek1@barnesandnoble.com","student_gender":"Polygender","student_phone_numbers":["9526294997","4699651256","7167123799","7061046839","7013761528"],"student_address":{"street":"188 Meadow Vale Avenue","city":"Augusta","state":"Georgia","postal_code":"30919"}, "action": "I"},
-- MAGIC {"student_id":3,"student_first_name":"Richart","student_last_name":"Zimmer","student_email":"rzimmer2@ox.ac.uk","student_gender":"Non-binary","student_phone_numbers":["3129072019","2815879465","9793774370","6367833815"],"student_address":{"street":"87155 Lunder Court","city":"Fort Myers","state":"Florida","postal_code":"33994"}, "action": "I"},
-- MAGIC {"student_id":4,"student_first_name":"Elyse","student_last_name":"Addionisio","student_email":"","student_gender":"Polygender","student_phone_numbers":["7347984926","3364474838","7136381150"],"student_address":{"street":"77 Sugar Alley","city":"Atlanta","state":"Georgia","postal_code":"31132"}, "action": "I"},
-- MAGIC {"student_id":5,"student_first_name":"Lilian","student_last_name":"Warret","student_email":"","student_gender":"Male","student_phone_numbers":["5031246553","6151432197","2152754201"],"student_address":{"street":"82540 Summer Ridge Point","city":"Sioux Falls","state":"South Dakota","postal_code":"57193"}, "action": "I"}
-- MAGIC ]}
-- MAGIC """
-- MAGIC import json
-- MAGIC from pyspark.sql import Row
-- MAGIC
-- MAGIC students1 = json.loads(students1_str)
-- MAGIC students1_df = spark.createDataFrame(Row(**x) for x in students1["students"])
-- MAGIC
-- MAGIC display(students1_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC students2_str = """
-- MAGIC {"students": [
-- MAGIC {"student_id":4,"student_first_name":"Elyse","student_last_name":"Addionisio","student_email":"eaddionisio3@berkeley.edu","student_gender":"Polygender","student_phone_numbers":["7347984926","3364474838","7136381150"],"student_address":{"street":"77 Sugar Alley","city":"Atlanta","state":"Georgia","postal_code":"31132"}, "action": "U"},
-- MAGIC {"student_id":5,"student_first_name":"Lilian","student_last_name":"Warret","student_email":"lwarret4@nsw.gov.au","student_gender":"Male","student_phone_numbers":["5031246553","6151432197","2152754201"],"student_address":{"street":"82540 Summer Ridge Point","city":"Sioux Falls","state":"South Dakota","postal_code":"57193"}, "action": "D"},
-- MAGIC {"student_id":6,"student_first_name":"Tate","student_last_name":"Swyne","student_email":"tswyne5@hud.gov","student_gender":"Agender","student_phone_numbers":["2021437429","8507115330","3047568052","7818031186","6072847440"],"student_address":{"street":"23 Sommers Parkway","city":"El Paso","state":"Texas","postal_code":"88569"}, "action": "I"},
-- MAGIC {"student_id":7,"student_first_name":"Ichabod","student_last_name":"Moring","student_email":"imoring6@un.org","student_gender":"Female","student_phone_numbers":["7147001301","9895085931"],"student_address":{"street":"584 Reindahl Way","city":"Denver","state":"Colorado","postal_code":"80228"}, "action": "I"},
-- MAGIC {"student_id":8,"student_first_name":"Ariel","student_last_name":"Howler","student_email":"ahowler7@tinypic.com","student_gender":"Agender","student_phone_numbers":null,"student_address":{"street":null,"city":null,"state":null,"postal_code":null}, "action": "I"},
-- MAGIC {"student_id":9,"student_first_name":"Octavia","student_last_name":"Stenner","student_email":"ostenner8@networksolutions.com","student_gender":"Bigender","student_phone_numbers":null,"student_address":{"street":null,"city":null,"state":null,"postal_code":null}, "action": "I"},
-- MAGIC {"student_id":10,"student_first_name":"Ronda","student_last_name":"Stean","student_email":"rstean9@xrea.com","student_gender":"Genderfluid","student_phone_numbers":null,"student_address":{"street":null,"city":null,"state":null,"postal_code":null}, "action": "I"}]}
-- MAGIC """
-- MAGIC
-- MAGIC import json
-- MAGIC from pyspark.sql import Row
-- MAGIC
-- MAGIC students2 = json.loads(students2_str)
-- MAGIC students2_df = spark.createDataFrame(Row(**x) for x in students2["students"])
-- MAGIC
-- MAGIC display(students2_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC students1_df.printSchema()

-- COMMAND ----------

-- MAGIC %fs rm -r /FileStore/delta/students

-- COMMAND ----------

DROP TABLE IF EXISTS students_db.students;

CREATE TABLE students_db.students(
    student_id STRING,
    student_first_name STRING,
    student_last_name STRING,
    student_email STRING,
    student_gender STRING,
    student_phone_numbers ARRAY<STRING>,
    student_address MAP<STRING, STRING>,
    action STRING
) USING DELTA 
LOCATION '/FileStore/delta/students'

-- COMMAND ----------

-- MAGIC %fs ls /FileStore/delta/students

-- COMMAND ----------

-- MAGIC %python
-- MAGIC students1_df.display()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC students1_df.write.insertInto("students_db.students")

-- COMMAND ----------

-- DBTITLE 1,TARGET
SELECT * FROM students

-- COMMAND ----------

-- MAGIC %python
-- MAGIC students2_df.createOrReplaceTempView("students2")

-- COMMAND ----------

-- DBTITLE 1,SOURCE
SELECT * FROM students2

-- COMMAND ----------

MERGE INTO students AS T 
USING students2 AS S 
ON T.student_id = S.student_id
WHEN MATCHED AND T.action = "D" THEN
  DELETE
WHEN MATCHED AND T.action = "U" THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------


