# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5f48b752-8972-44bf-8312-1fa1d43a6b76",
# META       "default_lakehouse_name": "bronze_layer",
# META       "default_lakehouse_workspace_id": "cba38f54-3fe7-486e-a341-b3fbdb58bdc2",
# META       "known_lakehouses": [
# META         {
# META           "id": "5f48b752-8972-44bf-8312-1fa1d43a6b76"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

df = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")


pat = r"/(event|fight|fighter)-details/[0-9a-f]+$"
for c in ["event_url", "fight_url", "f_1_fighter_url", "f_2_fighter_url"]:
    print(c, "| nulls:", df.filter(F.col(c).isNull()).count(),
          "| bad format:", df.filter(F.col(c).isNotNull() & ~F.col(c).rlike(pat)).count(),
          "| distinct:", df.select(c).distinct().count())

print("f_1_url == f_1_fighter_url:", df.filter(F.col("f_1_url") == F.col("f_1_fighter_url")).count(), "of", df.count())
df.select("f_1_url", "f_1_fighter_url", "f_1_name", "f_1_fighter_f_name", "f_1_fighter_l_name").show(3, truncate=False)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

from pyspark.sql import functions as F

#Construct natural keys from the urls of figthr, event and fighter_1/_2 columns in the kaggle dataset

df = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")

df = (
    df
    .withColumn(
        "fight_id",
        F.regexp_extract("fight_url", r"/fight-details/([0-9a-f]+)$", 1)
    )
    .withColumn(
        "event_id",
        F.regexp_extract("event_url", r"/event-details/([0-9a-f]+)$", 1)
    )
    .withColumn(
        "f1_fighter_id",
        F.regexp_extract("f_1_url", r"/fighter-details/([0-9a-f]+)$", 1)
    )
    .withColumn(
        "f2_fighter_id",
        F.regexp_extract("f_2_url", r"/fighter-details/([0-9a-f]+)$", 1)
    )
)

#Confirm that the keys exist
display(
    df.select(
        "fight_url",
        "fight_id",
        "event_url",
        "event_id",
        "f_1_url",
        "f1_fighter_id",
        "f_2_url",
        "f2_fighter_id"
    )
)

for c in ["fight_id", "event_id", "f1_fighter_id", "f2_fighter_id"]:
    print(c, df.filter(F.col(c) == "").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

#Converting event_date to actual date.
df = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")

df = df.withColumn("event_date", F.to_date("event_date"))
df.select(F.min("event_date"), F.max("event_date")).show()

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
