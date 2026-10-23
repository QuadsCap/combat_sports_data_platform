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

# MARKDOWN ********************

# ## 10_ingest_kagglesk — Landing files → Bronze
# 
# Loads the scarekrow Kaggle CSV (gap fill 16 May → 10 Oct) into a bronze Delta table, untouched, with load metadata. Second Kaggle source, separate from jerzyszocik (`source = 'kaggle'`).
# 
# - **Reads:** `Files/kaggle_file_load/<LOAD_ID>/UFC_full_data_silver_v2.csv`, uploaded by hand from Kaggle (scarekrow/ufc-data)
# - **Writes:** `bronze_layer.dbo.bronze_kagglesk_full_data_silver_v2`
# - **Run order:** after `10_ingest_kaggle`; set `LOAD_ID` to the landing folder date before running
# - **Metadata added:** `_ingested_at`, `_source_file`, `_load_id`. Source columns are not changed
# - **Read options:** CSV with `header`, `multiLine`, `escape='"'` (text fields can hold line breaks); no schema inference, all columns land as strings and get cast in `20_clean_kagglesk`
# - **Idempotent:** append mode, but a `LOAD_ID` that already exists in the table is skipped
# - **Limitation:** the file holds the full history (~8,937 fights), not just the gap; bronze keeps all of it, and the 16 May → 10 Oct filter happens in `20_clean_kagglesk`


# CELL ********************

notebookutils.fs.ls("Files/kagglesk_file_load/2026-10-22/UFC_full_data_silver_v2.csv")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df1 = spark.read.csv("Files/kagglesk_file_load/2026-10-22/UFC_full_data_silver_v2.csv")
display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = spark.read.csv("Files/kagglesk_file_load/2026-10-22/UFC_full_data_silver_v2.csv")
df2.printSchema()  
df2.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

LOAD_ID = "2026-10-22"
LANDING = f"Files/kagglesk_file_load/{LOAD_ID}"

# CSV file to ingest and its destination Bronze table
FILES = {
    "UFC_full_data_silver_v2.csv": "bronze_kagglesk_full_data_silver_v2",
}

for file_name, table in FILES.items():
    # Read the raw CSV and add ingestion metadata
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        # A quoted field can contain line breaks (e.g. result_details): read it as one value
        # instead of ending the row there. Without this, 270 fights were split across 2 rows.
        .option("multiLine", "true")
        # Quotes inside a quoted field are written as "" in this file, not \"
        .option("escape", '"')
        .csv(f"{LANDING}/{file_name}")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(f"{LANDING}/{file_name}"))
        .withColumn("_load_id", F.lit(LOAD_ID))
    )

    # Skip the load if this LOAD_ID already exists in the table
    already_loaded = (
        spark.catalog.tableExists(table)
        and spark.table(table)
            .filter(F.col("_load_id") == LOAD_ID)
            .limit(1)
            .count() > 0
    )

    if already_loaded:
        print(f"{table}: load {LOAD_ID} already exists, skipping")
    else:
        df.write.format("delta").mode("append").saveAsTable(table)
        print(f"{table}: load {LOAD_ID} written")

    # Verify the destination table
    t = spark.table(table)
    print(f"  rows: {t.count()}, columns: {len(t.columns)}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Compare column names between the existing Kaggle table and the new Kagglesk table

OLD_TABLE = "bronze_kaggle_full_data_silver_plus"
NEW_TABLE = "bronze_kagglesk_full_data_silver_v2"

METADATA_COLUMNS = {"_ingested_at", "_source_file", "_load_id"}

# Get the column names and exclude ingestion metadata
old_columns = set(spark.table(OLD_TABLE).columns) - METADATA_COLUMNS
new_columns = set(spark.table(NEW_TABLE).columns) - METADATA_COLUMNS

# Compare both directions
missing_columns = sorted(old_columns - new_columns)
new_columns_only = sorted(new_columns - old_columns)
matching_columns = sorted(old_columns & new_columns)

# Print the results
print(f"Existing table: {OLD_TABLE}")
print(f"New table:      {NEW_TABLE}")
print(f"\nMatching columns: {len(matching_columns)}")
print(f"Missing from new table: {len(missing_columns)}")
print(f"New or renamed columns: {len(new_columns_only)}")

print("\n--- Missing from Kagglesk (present in old table) ---")
for col in missing_columns:
    print(col)

print("\n--- New or renamed in Kagglesk (not in old table) ---")
for col in new_columns_only:
    print(col)

print("\n--- Matching columns ---")
for col in matching_columns:
    print(col)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
# Checks duplicates, broken URLs, invalid dates, and the number of fights before and after 16 May 2026.
# Define the source tables and load both Bronze datasets
OLD_TABLE = "bronze_kaggle_full_data_silver_plus"
NEW_TABLE = "bronze_kagglesk_full_data_silver_v2"

METADATA_COLUMNS = ["_ingested_at", "_source_file", "_load_id"]

old_df = spark.table(OLD_TABLE)
new_df = spark.table(NEW_TABLE)

# Find the last event date available in the original Kaggle dataset
old_max_date = (
    old_df.select(F.max(F.to_date("event_date")).alias("max_date"))
    .first()["max_date"]
)

if old_max_date is None:
    raise ValueError("Could not determine the maximum event_date in the old table.")

CUTOFF_DATE = str(old_max_date)

print(f"Last date in old Kaggle data: {CUTOFF_DATE}")
print(f"Expected new coverage: dates after {CUTOFF_DATE}")

# Compare total rows with distinct fight URLs to identify duplicate records
total_rows = new_df.count()
distinct_fights = new_df.select("fight_url").distinct().count()

print("\n=== ROW AND DUPLICATE CHECK ===")
print(f"Total rows: {total_rows}")
print(f"Distinct fight URLs: {distinct_fights}")
print(f"Rows beyond distinct URLs: {total_rows - distinct_fights}")

# Check whether repeated records are exact copies after excluding ingestion metadata
data_columns = [c for c in new_df.columns if c not in METADATA_COLUMNS]
distinct_data_rows = new_df.select(*data_columns).distinct().count()

print(f"Distinct data rows: {distinct_data_rows}")
print(f"Rows beyond distinct data rows: {total_rows - distinct_data_rows}")

# Find rows with missing, empty, or invalid fight URLs
broken_urls = new_df.filter(
    F.col("fight_url").isNull()
    | (F.trim(F.col("fight_url")) == "")
    | (~F.lower(F.trim(F.col("fight_url"))).startswith("http"))
)

print(f"Rows with broken URLs: {broken_urls.count()}")
display(broken_urls.limit(50))

# Identify URLs appearing more than once, including their occurrence counts
duplicate_urls = (
    new_df.filter(F.col("fight_url").isNotNull())
    .groupBy("fight_url")
    .count()
    .filter(F.col("count") > 1)
    .orderBy(F.desc("count"))
)

print(f"Duplicate URL groups: {duplicate_urls.count()}")
display(duplicate_urls.limit(50))

# Parse event dates and flag non-null values that cannot be interpreted as dates
dated_df = new_df.withColumn("_event_date", F.to_date("event_date"))

invalid_dates = dated_df.filter(
    F.col("event_date").isNotNull() & F.col("_event_date").isNull()
)

print(f"Rows with unparseable event dates: {invalid_dates.count()}")
display(invalid_dates.select("fight_url", "event_date").limit(50))

# Count rows and distinct fights on or before the cutoff versus after it
date_summary = dated_df.withColumn(
    "_period",
    F.when(F.col("_event_date") <= F.lit(CUTOFF_DATE).cast("date"), "Kaggle period")
     .when(F.col("_event_date") > F.lit(CUTOFF_DATE).cast("date"), "New period")
     .otherwise("Unknown date")
).groupBy("_period").agg(
    F.count("*").alias("total_rows"),
    F.countDistinct("fight_url").alias("distinct_fights")
).orderBy("_period")

print("\n=== DATE DISTRIBUTION ===")
display(date_summary)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Identifies fights that exist only in the new scrape or only in the existing dataset.
# Create distinct sets of non-null fight URLs for both datasets
old_urls = old_df.filter(F.col("fight_url").isNotNull()).select("fight_url").distinct()
new_urls = new_df.filter(F.col("fight_url").isNotNull()).select("fight_url").distinct()

# Find fights added by the new scrape and fights missing from it
new_only = new_urls.join(old_urls, on="fight_url", how="left_anti")
old_only = old_urls.join(new_urls, on="fight_url", how="left_anti")

print("=== FIGHT URL COMPARISON ===")
print(f"Fights only in new scrape: {new_only.count()}")
print(f"Fights only in old dataset: {old_only.count()}")

# Display sample fights that need investigation
print("=== NEW FIGHTS ===")
display(new_only.limit(100))

print("=== FIGHTS MISSING FROM NEW SCRAPE ===")
display(old_only.limit(100))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Compares all shared columns for fights that occur exactly once in each table. It first summarises the number of differences per column, then displays individual mismatched records.
# Exclude ingestion metadata and the fight URL join key from value comparisons
METADATA_COLUMNS = {"_ingested_at", "_source_file", "_load_id"}

shared_columns = sorted(
    (set(old_df.columns) & set(new_df.columns))
    - METADATA_COLUMNS
    - {"fight_url"}
)

print(f"Shared columns to compare: {len(shared_columns)}")
print(shared_columns)

# Keep only fight URLs occurring exactly once, avoiding duplicate-driven join multiplication
def unique_fights(df):
    unique_urls = (
        df.filter(F.col("fight_url").isNotNull())
        .groupBy("fight_url")
        .count()
        .filter(F.col("count") == 1)
        .select("fight_url")
    )
    return df.join(unique_urls, on="fight_url", how="inner")

old_unique = unique_fights(old_df).alias("old")
new_unique = unique_fights(new_df).alias("new")

# Match the same fights across both datasets
joined = old_unique.join(new_unique, on="fight_url", how="inner")

print(f"Unambiguous matching fights: {joined.count()}")

# Count mismatched values in each shared column, treating two nulls as equal
mismatch_counts = []

for col_name in shared_columns:
    mismatch = ~F.col(f"old.`{col_name}`").eqNullSafe(F.col(f"new.`{col_name}`"))
    mismatch_counts.append(
        F.sum(F.when(mismatch, 1).otherwise(0)).alias(col_name)
    )

if mismatch_counts:
    # Convert the summary into one row per column and sort by mismatch count
    mismatch_summary = joined.agg(*mismatch_counts)

    mismatch_summary = mismatch_summary.select(
        F.explode(
            F.array(*[
                F.struct(
                    F.lit(c).alias("column"),
                    F.col(c).alias("mismatch_count")
                )
                for c in shared_columns
            ])
        ).alias("result")
    ).select("result.*").orderBy(F.desc("mismatch_count"))

    print("=== MISMATCHES BY COLUMN ===")
    display(mismatch_summary)

    # Identify the exact columns that differ for each fight
    mismatch_flags = [
        F.when(
            ~F.col(f"old.`{c}`").eqNullSafe(F.col(f"new.`{c}`")),
            F.lit(c)
        )
        for c in shared_columns
    ]

    mismatched_fights = (
        joined
        .withColumn(
            "mismatched_columns",
            F.filter(F.array(*mismatch_flags), lambda x: x.isNotNull())
        )
        .filter(F.size("mismatched_columns") > 0)
    )

    print(f"Fights with at least one mismatch: {mismatched_fights.count()}")

    # Show old and new values side by side for detailed investigation
    comparison_columns = [
        expr
        for c in shared_columns
        for expr in (
            F.col(f"old.`{c}`").alias(f"old_{c}"),
            F.col(f"new.`{c}`").alias(f"new_{c}")
        )
    ]

    display(
        mismatched_fights.select(
            "fight_url",
            "mismatched_columns",
            *comparison_columns
        ).limit(100)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
