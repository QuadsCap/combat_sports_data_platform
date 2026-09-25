# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "4a5e2aa6-ba0e-4753-95e1-996a48d4169d",
# META       "default_lakehouse_name": "Bronze_Check",
# META       "default_lakehouse_workspace_id": "cba38f54-3fe7-486e-a341-b3fbdb58bdc2",
# META       "known_lakehouses": [
# META         {
# META           "id": "4a5e2aa6-ba0e-4753-95e1-996a48d4169d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df1 = spark.read.parquet("Files/full_data_silver_plus.parquet")
display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = spark.read.parquet("Files/ufc_features.parquet")
display(df2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = spark.read.parquet("Files/full_data_silver_plus.parquet")
print("Rows:", df1.count())
print("Columns:", len(df1.columns))

df2 = spark.read.parquet("Files/ufc_features.parquet")
print("Rows:", df2.count())
print("Columns:", len(df2.columns))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = spark.read.parquet("Files/full_data_silver_plus.parquet")
df2 = spark.read.parquet("Files/ufc_features.parquet")

display(
    df1.selectExpr(
        "min(event_date) as start_date",
        "max(event_date) as end_date"
    )
)


display(
    df2.selectExpr(
        "min(event_date) as start_date",
        "max(event_date) as end_date"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
