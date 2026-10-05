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

df1 = spark.read.parquet("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")
display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = spark.read.parquet("Files/kaggle_file_load/2026-09-24/ufc_features.parquet")
display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df = spark.table("bronze_kaggle_full_data_silver_plus")
print(df.count(), len(df.columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_cols = [c for c in df.columns if "date" in c.lower()]

print(date_cols)
df.select(date_cols).printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df = spark.read.parquet(
    "Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet"
)

stats = ["fighter_w", "fighter_l", "fighter_d", "fighter_nc_dq", "fighter_SApM", "fighter_SlpM",
         "fighter_Str_Acc", "fighter_Str_Def", "fighter_Sub_Avg", "fighter_TD_Acc", "fighter_TD_Avg",
         "fighter_TD_Def", "ranking", "fighter_weight_lbs", "fighter_height_cm", "fighter_reach_cm"]

def corner(n):
    return df.select(F.col(f"f_{n}_fighter_url").alias("fid"), *[F.col(f"f_{n}_{s}").alias(s) for s in stats])

per_fighter = (corner(1).unionByName(corner(2))
               .groupBy("fid")
               .agg(F.count("*").alias("n"), *[F.countDistinct(s).alias(s) for s in stats])
               .filter("n >= 3"))

# % of fighters (3+ fights) whose value CHANGES across their fights
per_fighter.select(F.count("*").alias("fighters"),
                   *[F.round(F.avg((F.col(s) > 1).cast("int")) * 100, 1).alias(s) for s in stats]
                  ).show(vertical=True)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

df = spark.read.parquet(
    "Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet"
)

for c in sorted(df.columns):
    print(c)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
