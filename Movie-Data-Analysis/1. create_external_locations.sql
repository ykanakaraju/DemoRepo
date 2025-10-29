-- Databricks notebook source
CREATE EXTERNAL LOCATION IF NOT EXISTS movielens_landing
URL "abfss://landing@demoykrucdemodbsa.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `demoykrucdemodbsa_cred`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS movielens_bronze
URL "abfss://bronze@demoykrucdemodbsa.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `demoykrucdemodbsa_cred`);

-- COMMAND ----------

DESC EXTERNAL LOCATION movielense_bronze;

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS movielens_silver
URL "abfss://silver@demoykrucdemodbsa.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `demoykrucdemodbsa_cred`);

-- COMMAND ----------

CREATE EXTERNAL LOCATION IF NOT EXISTS movielens_gold
URL "abfss://gold@demoykrucdemodbsa.dfs.core.windows.net/"
WITH (STORAGE CREDENTIAL `demoykrucdemodbsa_cred`);

-- COMMAND ----------

SHOW EXTERNAL LOCATIONS

-- COMMAND ----------


