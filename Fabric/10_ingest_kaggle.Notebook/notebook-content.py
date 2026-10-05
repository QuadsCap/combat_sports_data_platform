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

notebookutils.fs.ls("Files/kaggle_file_load/2026-09-24/full_data_silver_plus.parquet")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

df2 = spark.read.parquet("Files/kaggle_file_load/2026-09-24/ufc_features.parquet")
df2.printSchema()  
df2.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Guard cell

print(spark.table(TABLE).count())
print(len(spark.table(TABLE).columns))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F


LOAD_ID = "2026-09-24"
LANDING = f"Files/kaggle_file_load/{LOAD_ID}"

# Files to ingest, and the bronze table each one lands in
FILES = {
    "full_data_silver_plus.parquet": "bronze_kaggle_full_data_silver_plus",
    "ufc_features.parquet":          "bronze_kaggle_ufc_features",
}

for file_name, table in FILES.items():
    # Read the raw file and add the metadata columns (source data is not changed)
    df = (
        spark.read.parquet(f"{LANDING}/{file_name}")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_load_id", F.lit(LOAD_ID))
    )

    # Guard: checks if the table exists and already has data with this LOAD_ID in the _load_id column
    already_loaded = (
        spark.catalog.tableExists(table)
        and spark.table(table).filter(F.col("_load_id") == LOAD_ID).count() > 0
    )

    if already_loaded:
        print(f"{table}: load {LOAD_ID} already exists, skipping")
    else:
        df.write.format("delta").mode("append").saveAsTable(table)
        print(f"{table}: load {LOAD_ID} written")

    # Verify: check the table has the expected rows and columns
    t = spark.table(table)
    print(f"  rows: {t.count()}, columns: {len(t.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import re
from collections import Counter

df = spark.read.table("bronze_kaggle_full_data_silver_plus")

# Strip corner prefix (f_1_ / f_2_) and round prefix (r1_ ... r5_)
base = [re.sub(r"^r\d_", "", re.sub(r"^f_[12]_", "", c)) for c in df.columns]

counts = Counter(base)
print(f"{len(df.columns)} columns -> {len(counts)} distinct base names\n")
for name, n in sorted(counts.items()):
    print(f"{n:>3}  {name}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
