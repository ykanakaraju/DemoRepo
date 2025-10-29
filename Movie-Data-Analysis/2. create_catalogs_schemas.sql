-- Databricks notebook source
CREATE CATALOG IF NOT EXISTS movielens_dev;

-- COMMAND ----------

USE CATALOG movielens_dev;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS movielens_landing
MANAGED LOCATION "abfss://landing@demoykrucdemodbsa.dfs.core.windows.net/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS movielens_bronze
MANAGED LOCATION "abfss://bronze@demoykrucdemodbsa.dfs.core.windows.net/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS movielens_silver
MANAGED LOCATION "abfss://silver@demoykrucdemodbsa.dfs.core.windows.net/"

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS movielens_gold
MANAGED LOCATION "abfss://gold@demoykrucdemodbsa.dfs.core.windows.net/"

-- COMMAND ----------


