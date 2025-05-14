# Databricks notebook source
dataPath = "dbfs:/FileStore/delta/students"

dbutils.fs.rm(dataPath, True)

# COMMAND ----------

students1_str = """
{"students": [{"student_id":1,"student_first_name":"Eduino","student_last_name":"Dawdry","student_email":"edawdry0@whitehouse.gov","student_gender":"Bigender","student_phone_numbers":["5737119029"],"student_address":{"street":"218 Ridgeway Crossing","city":"Omaha","state":"Nebraska","postal_code":"68110"}, "action": "I"},
{"student_id":2,"student_first_name":"Lacee","student_last_name":"Prosek","student_email":"lprosek1@barnesandnoble.com","student_gender":"Polygender","student_phone_numbers":["9526294997","4699651256","7167123799","7061046839","7013761528"],"student_address":{"street":"188 Meadow Vale Avenue","city":"Augusta","state":"Georgia","postal_code":"30919"}, "action": "I"},
{"student_id":3,"student_first_name":"Richart","student_last_name":"Zimmer","student_email":"rzimmer2@ox.ac.uk","student_gender":"Non-binary","student_phone_numbers":["3129072019","2815879465","9793774370","6367833815"],"student_address":{"street":"87155 Lunder Court","city":"Fort Myers","state":"Florida","postal_code":"33994"}, "action": "I"},
{"student_id":4,"student_first_name":"Elyse","student_last_name":"Addionisio","student_email":"","student_gender":"Polygender","student_phone_numbers":["7347984926","3364474838","7136381150"],"student_address":{"street":"77 Sugar Alley","city":"Atlanta","state":"Georgia","postal_code":"31132"}, "action": "I"},
{"student_id":5,"student_first_name":"Lilian","student_last_name":"Warret","student_email":"","student_gender":"Male","student_phone_numbers":["5031246553","6151432197","2152754201"],"student_address":{"street":"82540 Summer Ridge Point","city":"Sioux Falls","state":"South Dakota","postal_code":"57193"}, "action": "I"}
]}
"""

# COMMAND ----------

students2_str = """
{"students": [
{"student_id":4,"student_first_name":"Elyse","student_last_name":"Addionisio","student_email":"eaddionisio3@berkeley.edu","student_gender":"Polygender","student_phone_numbers":["7347984926","3364474838","7136381150"],"student_address":{"street":"77 Sugar Alley","city":"Atlanta","state":"Georgia","postal_code":"31132"}, "action": "U"},
{"student_id":5,"student_first_name":"Lilian","student_last_name":"Warret","student_email":"lwarret4@nsw.gov.au","student_gender":"Male","student_phone_numbers":["5031246553","6151432197","2152754201"],"student_address":{"street":"82540 Summer Ridge Point","city":"Sioux Falls","state":"South Dakota","postal_code":"57193"}, "action": "D"},
{"student_id":6,"student_first_name":"Tate","student_last_name":"Swyne","student_email":"tswyne5@hud.gov","student_gender":"Agender","student_phone_numbers":["2021437429","8507115330","3047568052","7818031186","6072847440"],"student_address":{"street":"23 Sommers Parkway","city":"El Paso","state":"Texas","postal_code":"88569"}, "action": "I"},
{"student_id":7,"student_first_name":"Ichabod","student_last_name":"Moring","student_email":"imoring6@un.org","student_gender":"Female","student_phone_numbers":["7147001301","9895085931"],"student_address":{"street":"584 Reindahl Way","city":"Denver","state":"Colorado","postal_code":"80228"}, "action": "I"},
{"student_id":8,"student_first_name":"Ariel","student_last_name":"Howler","student_email":"ahowler7@tinypic.com","student_gender":"Agender","student_phone_numbers":null,"student_address":{"street":null,"city":null,"state":null,"postal_code":null}, "action": "I"},
{"student_id":9,"student_first_name":"Octavia","student_last_name":"Stenner","student_email":"ostenner8@networksolutions.com","student_gender":"Bigender","student_phone_numbers":null,"student_address":{"street":null,"city":null,"state":null,"postal_code":null}, "action": "I"},
{"student_id":10,"student_first_name":"Ronda","student_last_name":"Stean","student_email":"rstean9@xrea.com","student_gender":"Genderfluid","student_phone_numbers":null,"student_address":{"street":null,"city":null,"state":null,"postal_code":null}, "action": "I"}]}
"""

# COMMAND ----------

import json
from pyspark.sql import Row

students1 = json.loads(students1_str)
students1_df = spark.createDataFrame(Row(**x) for x in students1['students'])

students2 = json.loads(students2_str)
students2_df = spark.createDataFrame(Row(**x) for x in students2['students'])


# COMMAND ----------

students1_df.write.format('delta').save(dataPath)

# COMMAND ----------

from delta.tables import DeltaTable

students_delta = DeltaTable.forPath(spark, dataPath)

# COMMAND ----------

# DBTITLE 1,TARGET - DeltaTable
display(students_delta.toDF())

# COMMAND ----------

# DBTITLE 1,SOURCE: DataFrame
display(students2_df)

# COMMAND ----------

students_delta.toDF().printSchema()

# COMMAND ----------

help(students_delta.merge)

# COMMAND ----------

merge_condition = students_delta.alias("t")\
    .merge(
        source = students2_df.alias("s"),
        condition = "s.student_id = t.student_id"
    )

# COMMAND ----------

type(merge_condition)

# COMMAND ----------

help(merge_condition)

# COMMAND ----------

help(merge_condition.whenMatchedDelete)

# COMMAND ----------

students_delta.alias("t")\
  .merge(
    source = students2_df.alias("s"),
    condition = 's.student_id = t.student_id'
  ) \
  .whenMatchedDelete(
    condition = 's.action = "D"'
  ) \
  .whenMatchedUpdateAll(
    condition = 's.action = "U"'
  ) \
  .whenNotMatchedInsertAll() \
  .execute()

# COMMAND ----------

students_delta.toDF().display()

# COMMAND ----------

students_delta.history().display()

# COMMAND ----------



# COMMAND ----------


