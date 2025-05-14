# Databricks notebook source
# MAGIC %md
# MAGIC [Documentation](https://docs.databricks.com/aws/en/visualizations)

# COMMAND ----------

movies_file = "dbfs:/FileStore/ctsdatasets/movielens/movies.csv"
ratings_file = "dbfs:/FileStore/ctsdatasets/movielens/ratings.csv"

# COMMAND ----------

movies_schema = "movieId INT, title STRING, genres STRING"
ratings_schema = "userId INT, movieId INT, rating DOUBLE, timestamp BIGINT"

# COMMAND ----------

movies_df = spark.read.csv(movies_file, header=True, schema=movies_schema)
ratings_df = spark.read.csv(ratings_file, header=True, schema=ratings_schema)

# COMMAND ----------

display(movies_df.limit(5))

# COMMAND ----------

display(ratings_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top 10 movies with highest average user rating

# COMMAND ----------

top_movies_df = (
    ratings_df
    .groupBy("movieId")
    .agg(
        count("rating").alias("totalRatings"),
        avg("rating").alias("averageRating")
    )
    .where("totalRatings >= 50")
    .orderBy( desc("averageRating") )
    .limit(10)
    .join(movies_df, "movieId")
    .select("movieId", "title", "totalRatings", "averageRating")
    .orderBy( desc("averageRating") )
    .withColumn("averageRating", round("averageRating", 4))
    .coalesce(1)
)

# COMMAND ----------

display(top_movies_df)
# top_movies_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Movies by Year

# COMMAND ----------

import re
from pyspark.sql.types import *

# COMMAND ----------


def get_year_from_title(title):
    try:
        year = int(re.findall("\((.+?)\)", title)[-1])
    except:
        year = 0
    return year 

# COMMAND ----------

get_year_from_title("Toy Story (1995)")

# COMMAND ----------

get_year_from_title_udf = udf(get_year_from_title, returnType=IntegerType())

# COMMAND ----------

movies_year_df = (
    movies_df
    .withColumn("year", get_year_from_title_udf(col("title")))
    .drop(movies_df.genres)
)

# COMMAND ----------

yearly_movies_df = movies_year_df \
    .groupBy("year").count()\
    .orderBy(desc("count"), asc("year")) \
    .limit(15) \
    .withColumnRenamed("count", "number_of_movies")

# COMMAND ----------

display(yearly_movies_df)

# COMMAND ----------


