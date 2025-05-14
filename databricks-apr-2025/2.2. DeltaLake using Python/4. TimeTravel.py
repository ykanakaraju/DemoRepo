# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ####Time Travel - Rollback a Delta Lake Table to a Previous Version with 'Restore'####
# MAGIC
# MAGIC **Delta tables allows us to rollback our data to a previous snapshot based on a version or timestamp**
# MAGIC
# MAGIC When you’re working with a plain vanilla data lake, rolling back errors can be extremely challenging, if not impossible – especially if files were deleted. The ability to undo mistakes is a huge benefit that Delta Lake offers end users. Unlike, say, a plain vanilla Parquet table, Delta Lake preserves a history of the changes you make over time, storing different versions of your data. Rolling back your Delta Lake table to a previous version with the restore command can be a great way to reverse bad data inserts or undo an operation that mutated the table in unexpected ways.

# COMMAND ----------

# MAGIC %run "./3.3. Conditional Merge"

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


