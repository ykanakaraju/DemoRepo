# Databricks notebook source
dbutils.help()

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ***text* widget**

# COMMAND ----------

dbutils.widgets.help("text")

# COMMAND ----------

dbutils.widgets.text("name", "Kanak", "Name")
v_name = dbutils.widgets.get("name")
print(v_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ***dropdown* widget**

# COMMAND ----------

dbutils.widgets.dropdown("city", "", ["", "Chennai", "Pune", "Delhi", "Hyderabad", "Noida"], "City")
v_city = dbutils.widgets.get("city")
print(v_city)

# COMMAND ----------

# MAGIC %md
# MAGIC ***multiselect* widget**

# COMMAND ----------

dbutils.widgets.multiselect("hobbies", "TV", ["TV", "Reading", "Blogging", "Chess"], "Hobbies")
v_hobbies = dbutils.widgets.get("hobbies")
print(v_hobbies)

# COMMAND ----------

dbutils.widgets.remove("city")

# COMMAND ----------

dbutils.widgets.removeAll()

# COMMAND ----------

# MAGIC %md
# MAGIC - `dbutils.notebook.exit` - return a value to the calling module

# COMMAND ----------

return_value = v_name + "," + v_city
dbutils.notebook.exit(return_value)

# COMMAND ----------

# MAGIC %md
# MAGIC
