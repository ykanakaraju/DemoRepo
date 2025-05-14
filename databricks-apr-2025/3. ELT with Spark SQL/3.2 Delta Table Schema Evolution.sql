-- Databricks notebook source
DROP DATABASE IF EXISTS demodb CASCADE;
CREATE DATABASE IF NOT EXISTS demodb;
USE demodb;

-- COMMAND ----------

SELECT id, fname, lname FROM json.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

DROP TABLE IF EXISTS people;

CREATE OR REPLACE TABLE people(
  id INT,
  firstName STRING,
  lastName STRING
) USING DELTA;

INSERT INTO people
SELECT id, fname, lname FROM json.`dbfs:/FileStore/data/schema/people.json`;

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "false") 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Schema Validations Summary
-- MAGIC 1. INSERT INTO &emsp;&emsp;&emsp;&emsp;&emsp;&emsp;&ensp;&nbsp;- Column matching by position, New columns not allowed
-- MAGIC 2. INSERT OVERWRITE &emsp;&emsp;&emsp;&emsp;&ensp;- Column matching by position, New columns not allowed
-- MAGIC 3. MERGE INSERT &emsp;&emsp;&emsp;&nbsp;- Column matching by name, New columns ignored
-- MAGIC 4. DataFrame Append &emsp;&nbsp;- Column matching by name, New columns not allowed
-- MAGIC 5. Data Type Mismatch &emsp;- Not allowed in any case
-- MAGIC #####Schema evolution approaches
-- MAGIC 1. Manual&emsp;&nbsp; - New columns
-- MAGIC 2. Automatic - New columns

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####1. Manual schema evolution - New column at the end

-- COMMAND ----------

INSERT INTO people
SELECT id, fname, lname, dob
FROM json.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

ALTER TABLE people ADD COLUMNS (birthDate STRING);

-- COMMAND ----------

DESC people

-- COMMAND ----------

-- DBTITLE 1,Columns by position
INSERT INTO people
SELECT id, fname, lname, dob
FROM json.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

INSERT INTO people
SELECT id, fname firstName, lname lastName
FROM json.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

INSERT INTO people(id, firstName, lastName)
SELECT id, fname, lname
FROM json.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

desc people

-- COMMAND ----------

INSERT INTO people
SELECT id, fname firstName, lname lastName, dob birthDate, current_date() toDay
FROM json.`dbfs:/FileStore/data/schema/people.json`

-- COMMAND ----------

INSERT OVERWRITE people
SELECT id, fname firstName, lname lastName
FROM json.`dbfs:/FileStore/data/schema/people_2.json`

-- COMMAND ----------

INSERT OVERWRITE people(id, firstName, lastName)
SELECT id, fname firstName, lname lastName
FROM json.`dbfs:/FileStore/data/schema/people_2.json`

-- COMMAND ----------

select * from people

-- COMMAND ----------

DESC HISTORY people

-- COMMAND ----------

SELECT * FROM people VERSION AS OF 5

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####2. Manual schema evolution - New column in the middle

-- COMMAND ----------

ALTER TABLE people ADD COLUMNS (phoneNumber STRING after lastName);

-- COMMAND ----------

DESC people

-- COMMAND ----------

INSERT INTO people
SELECT id, fname firstName, lname lastName, phone phoneNumber, dob birthDate
FROM json.`dbfs:/FileStore/data/schema/people_2.json`

-- COMMAND ----------

select * from people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Cleanup and Setup for Automatic Schema Evolution

-- COMMAND ----------

DROP TABLE IF EXISTS people;

CREATE OR REPLACE TABLE people(
  id INT,
  firstName STRING,
  lastName STRING
) USING DELTA;

INSERT INTO people
SELECT id, fname, lname FROM json.`dbfs:/FileStore/data/schema/people.json`;

SELECT * FROM people;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Automatic Schema Evolution - At Session level

-- COMMAND ----------

SET spark.databricks.delta.schema.autoMerge.enabled = true

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####3. Automatic schema evolution - New column at the end

-- COMMAND ----------

desc people

-- COMMAND ----------

INSERT INTO people
SELECT id, fname firstName, lname lastName, dob birthDate
FROM json.`dbfs:/FileStore/data/schema/people_2.json` 

-- COMMAND ----------

select * from people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####4. Automatic schema evolution - New column in the middle
-- MAGIC For INSERT 
-- MAGIC 1. Either it doesn't work because of the column matching by position
-- MAGIC 2. Or it corrupts your data

-- COMMAND ----------

desc people

-- COMMAND ----------

INSERT INTO people
SELECT id, fname firstName, lname lastName, phone phoneNumber, dob birthDate
FROM json.`dbfs:/FileStore/data/schema/people_2.json`

-- COMMAND ----------

INSERT INTO people
SELECT id, fname firstName, lname lastName, phone phoneNumber
FROM json.`dbfs:/FileStore/data/schema/people_2.json`

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Automatic schema evolution - New column in the middle
-- MAGIC Works with MERGE INSERT

-- COMMAND ----------

MERGE INTO people T
USING 
(
    SELECT id, fname firstName, lname lastName, phone phoneNumber, dob birthDate 
    FROM json.`dbfs:/FileStore/data/schema/people_3.json`
) S
ON T.id = S.id
WHEN NOT MATCHED THEN 
    INSERT *

-- COMMAND ----------

select * from people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Cleanup and Setup for Automatic Schema Evolution at Table level

-- COMMAND ----------

DROP TABLE IF EXISTS people;

CREATE OR REPLACE TABLE people(
  id INT,
  firstName STRING,
  lastName STRING
) USING DELTA;

INSERT INTO people
SELECT id, fname, lname FROM json.`dbfs:/FileStore/data/schema/people.json`;

SELECT * FROM people;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####6. Schema evolution - New column at the end

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import to_date
-- MAGIC
-- MAGIC people_2_schema = "id INT, fname STRING, lname STRING, dob STRING"
-- MAGIC
-- MAGIC people_2_df =  (
-- MAGIC       spark
-- MAGIC       .read
-- MAGIC       .format("json")
-- MAGIC       .schema(people_2_schema)
-- MAGIC       .load("dbfs:/FileStore/data/schema/people_2.json")
-- MAGIC       .toDF("id", "firstName", "lastName", "birthDate")
-- MAGIC )
-- MAGIC
-- MAGIC display(people_2_df)

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC      people_2_df
-- MAGIC       .write
-- MAGIC       .format("delta")
-- MAGIC       .mode("append")
-- MAGIC       .option("mergeSchema", "true")
-- MAGIC       .saveAsTable("people")
-- MAGIC )

-- COMMAND ----------

select * from people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####5. Automatic schema evolution - New column in the middle

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import to_date
-- MAGIC
-- MAGIC people_3_schema = "id INT, fname STRING, lname STRING, phone STRING, dob STRING"
-- MAGIC
-- MAGIC people_3_df =  (
-- MAGIC       spark
-- MAGIC       .read
-- MAGIC       .format("json")
-- MAGIC       .schema(people_3_schema)
-- MAGIC       .load("dbfs:/FileStore/data/schema/people_3.json")
-- MAGIC       .toDF("id", "firstName", "lastName", "phoneNumber", "birthDate")
-- MAGIC )
-- MAGIC
-- MAGIC display(people_3_df)

-- COMMAND ----------

SELECT * FROM people

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (
-- MAGIC    people_3_df
-- MAGIC       .write
-- MAGIC       .format("delta")
-- MAGIC       .mode("append")
-- MAGIC       .option("mergeSchema", "true")
-- MAGIC       .saveAsTable("people")
-- MAGIC )

-- COMMAND ----------

select * from people

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Cleanup

-- COMMAND ----------

DROP DATABASE IF EXISTS demodb CASCADE

-- COMMAND ----------


