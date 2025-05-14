-- Databricks notebook source
-- MAGIC %fs ls dbfs:/FileStore/data/schema

-- COMMAND ----------

DROP DATABASE IF EXISTS demodb CASCADE;
CREATE DATABASE demodb;
USE demodb;

-- COMMAND ----------

CREATE OR REPLACE TABLE people (
  id INT,
  firstName STRING,
  lastName STRING
) USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Schema Validations Summary
-- MAGIC 1. `INSERT INTO`
-- MAGIC     - Column matching by position, New columns not allowed
-- MAGIC 2. `INSERT OVERWRITE`
-- MAGIC     - Column matching by position, New columns not allowed
-- MAGIC 3. `MERGE INSERT` 
-- MAGIC     - Column matching by name, New columns ignored
-- MAGIC 4. `DataFrame Append` 
-- MAGIC     - Column matching by name, New columns not allowed
-- MAGIC 5. `Data Type Mismatch` 
-- MAGIC     - Not allowed in any case

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####1. `INSERT INTO` & `INSERT OVERWRITE`
-- MAGIC
-- MAGIC - Column matching by position - matching column names not mandatory

-- COMMAND ----------

SELECT * FROM JSON.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

DESC people

-- COMMAND ----------

INSERT INTO people
SELECT id, fname, lname FROM JSON.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC - New columns not allowed

-- COMMAND ----------

INSERT INTO people
SELECT id, fname, lname, dob
FROM json.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

INSERT OVERWRITE people
SELECT id, fname, lname, dob
FROM json.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. MERGE

-- COMMAND ----------

desc people

-- COMMAND ----------

SELECT id, fname, lname FROM json.`dbfs:/FileStore/data/schema/people_2.json`

-- COMMAND ----------

-- DBTITLE 1,Column matching by name, not by position
MERGE INTO people T
USING (SELECT id, fname, lname FROM json.`dbfs:/FileStore/data/schema/people_2.json`) S
ON T.id = S.id
WHEN NOT MATCHED THEN INSERT *  

-- COMMAND ----------

-- DBTITLE 1,Column matching by name - allowed
MERGE INTO people T
USING 
( 
  SELECT id, fname firstName, lname lastName FROM json.`dbfs:/FileStore/data/schema/people_2.json`
) S
ON T.id = S.id
WHEN NOT MATCHED THEN INSERT *  

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

-- DBTITLE 1,Column matching by name - new columns ignored
SELECT id, fname firstName, lname lastName, dob 
FROM json.`dbfs:/FileStore/data/schema/people_3.json`

-- COMMAND ----------

MERGE INTO people T
USING 
(   
    SELECT id, fname firstName, lname lastName, dob FROM json.`dbfs:/FileStore/data/schema/people_3.json`
) S
ON T.id = S.id
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

select * from people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Dataframe append
-- MAGIC
-- MAGIC - Column matching is by name, not by position
-- MAGIC - New columns not allowed
-- MAGIC

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

-- DBTITLE 1,Matching by position - not allowed
-- MAGIC %python
-- MAGIC people_schema = "id INT, fname STRING, lname STRING"
-- MAGIC people_df =  spark.read.schema(people_schema).json("dbfs:/FileStore/data/schema/people_2.json")
-- MAGIC display(people_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC people_df.write.format("delta").mode("append").saveAsTable("people")

-- COMMAND ----------

-- DBTITLE 1,Matching by name - allowed
-- MAGIC %python
-- MAGIC people_schema = "id INT, fname STRING, lname STRING"
-- MAGIC
-- MAGIC people_df =  (
-- MAGIC   spark
-- MAGIC     .read
-- MAGIC     .schema(people_schema)
-- MAGIC     .json("dbfs:/FileStore/data/schema/people_2.json")
-- MAGIC     .withColumnRenamed("fname", "firstName")
-- MAGIC     .withColumnRenamed("lname", "lastName")
-- MAGIC )
-- MAGIC
-- MAGIC people_df.write.format("delta").mode("append").saveAsTable("people")

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

-- DBTITLE 1,New columns not allowed
-- MAGIC %python
-- MAGIC people_schema = "id INT, fname STRING, lname STRING, dob DATE"
-- MAGIC
-- MAGIC people_df =  (
-- MAGIC   spark
-- MAGIC     .read
-- MAGIC     .schema(people_schema)
-- MAGIC     .json("dbfs:/FileStore/data/schema/people_2.json")
-- MAGIC     .withColumnRenamed("fname", "firstName")
-- MAGIC     .withColumnRenamed("lname", "lastName")
-- MAGIC )
-- MAGIC
-- MAGIC display(people_df)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC people_df.write.format("delta").mode("append").saveAsTable("people")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC people_df.write.format("delta").mode("overwrite").saveAsTable("people")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Cleanup

-- COMMAND ----------

DROP DATABASE IF EXISTS demodb CASCADE

-- COMMAND ----------

-- MAGIC %fs rm -r dbfs:/user/hive/warehouse/demodb.db

-- COMMAND ----------


