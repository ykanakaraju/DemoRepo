# Databricks notebook source
dataset_bookstore = "dbfs:/FileStore/data/bookstore"
spark.conf.set(f"dataset.bookstore", dataset_bookstore)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Reading Stream

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMP VIEW books_csv_tmp_vw (
# MAGIC   book_id STRING,
# MAGIC   title STRING,
# MAGIC   author STRING,
# MAGIC   category STRING,
# MAGIC   price DOUBLE
# MAGIC )
# MAGIC USING CSV
# MAGIC OPTIONS( 
# MAGIC   path = "dbfs:/FileStore/data/bookstore/books-csv",
# MAGIC   header="true", 
# MAGIC   delimiter=";"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS books;
# MAGIC CREATE TABLE books AS SELECT * FROM books_csv_tmp_vw;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM books

# COMMAND ----------

books_df = spark.readStream.table("books")

# COMMAND ----------

books_df.createOrReplaceTempView("books_streaming_tmp_vw")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM books_streaming_tmp_vw

# COMMAND ----------

#display(books_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Applying Transformations

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT author, count(book_id) as total_books
# MAGIC FROM books_streaming_tmp_vw
# MAGIC GROUP BY author

# COMMAND ----------

# DBTITLE 1,ORDER BY not support (except in few cases)
# MAGIC %sql
# MAGIC SELECT * FROM books_streaming_tmp_vw ORDER BY author

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Persisting Streaming Data

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW author_counts_tmp_vw AS (
# MAGIC   SELECT author, count(book_id) as total_books
# MAGIC   FROM books_streaming_tmp_vw
# MAGIC   GROUP BY author
# MAGIC )
# MAGIC

# COMMAND ----------

(
  spark.table("author_counts_tmp_vw")
    .writeStream
    .trigger(processingTime="4 seconds")
    .outputMode("complete")
    .option("checkpointLocation", "/FileStore/checkpoint/author_counts")
    .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts

# COMMAND ----------

# MAGIC %md
# MAGIC #### Adding New Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC VALUES 
# MAGIC ("B19", "Introduction to Modeling and Simulation", "Mark W. Spong", "Computer Science", 25),
# MAGIC ("B20", "Robot Modeling and Control", "Mark W. Spong", "Computer Science", 30),
# MAGIC ("B21", "Turing's Vision: The Birth of Computer Science", "Chris Bernhardt", "Computer Science", 35)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Streaming in Batch Mode 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO books
# MAGIC VALUES 
# MAGIC ("B16", "Hands-On Deep Learning Algorithms with Python", "Sudharsan Ravichandiran", "Computer Science", 25),
# MAGIC ("B17", "Neural Network Methods in Natural Language Processing", "Yoav Goldberg", "Computer Science", 30),
# MAGIC ("B18", "Understanding digital signal processing", "Richard Lyons", "Computer Science", 35)

# COMMAND ----------

# DBTITLE 1,Batch Query - avialableNow=True
(
  spark.table("author_counts_tmp_vw")
    .writeStream
    .trigger(availableNow=True)
    .outputMode("complete")
    .option("checkpointLocation", "/FileStore/checkpoint/author_counts")
    .table("author_counts")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM author_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Terminate active streams

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping stream: " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------


