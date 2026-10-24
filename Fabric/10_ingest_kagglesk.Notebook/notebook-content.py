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

# =====================================================================
# Value comparison: jerzyszocik (old) vs kagglesk (new), shared fights
# ---------------------------------------------------------------------
# What:   counts per column how many fights differ, on fights where both
#         sources put the same fighter in the f1 corner.
# How:    each column is compared by the OLD table's type, because old is
#         typed (parquet) and new is all strings (CSV):
#           numeric -> both cast to double, equal within 0.01
#           other   -> text cleaned (whitespace incl. line breaks collapsed,
#                      trimmed, lowercased, '' treated as null)
# Expect: most columns 0; what stays high is a real difference.
# =====================================================================

from pyspark.sql import functions as F
from pyspark.sql.types import NumericType

METADATA_COLUMNS = {"_ingested_at", "_source_file", "_load_id"}

# Career stats + record are snapshots from the scrape date, refreshed by the
# July re-scrape -> they differ for every active fighter, so they say nothing about format
SNAPSHOT_SUFFIXES = (
    "_fighter_SApM", "_SlpM", "_Str_Acc", "_Str_Def",
    "_TD_Avg", "_TD_Acc", "_TD_Def", "_Sub_Avg",
    "_fighter_w", "_fighter_l", "_fighter_d", "_fighter_nc_dq",
)

shared_columns = sorted(
    (set(old_df.columns) & set(new_df.columns)) - METADATA_COLUMNS - {"fight_url"}
)
compare_columns = [c for c in shared_columns if not c.endswith(SNAPSHOT_SUFFIXES)]


def unique_fights(df):
    # Keep only fight_urls that occur once, so the join can't multiply rows
    unique_urls = (
        df.filter(F.col("fight_url").isNotNull())
        .groupBy("fight_url").count()
        .filter(F.col("count") == 1)
        .select("fight_url")
    )
    return df.join(unique_urls, on="fight_url", how="inner")


# f1/f2 are randomised independently per source: only compare fights where
# f1 is the same fighter in both, otherwise f_1_* vs f_1_* compares two people
joined = (
    unique_fights(old_df).alias("old")
    .join(unique_fights(new_df).alias("new"), on="fight_url", how="inner")
    .filter(F.col("old.f_1_fighter_url").eqNullSafe(F.col("new.f_1_fighter_url")))
)
print(f"Fights with matching f1 corners: {joined.count()}")


def clean_text(col):
    # F.trim only strips spaces, not \n -> collapse every whitespace run to one space first.
    # SQL: NULLIF(LOWER(TRIM(REGEXP_REPLACE(col, '\s+', ' '))), '')
    cleaned = F.lower(F.trim(F.regexp_replace(col.cast("string"), r"\s+", " ")))
    return F.when(cleaned != "", cleaned)   # no otherwise -> '' becomes null


def mismatch_expr(c):
    """True when old and new differ for column c (two nulls count as equal)."""
    old_col = F.col(f"old.`{c}`")
    new_col = F.col(f"new.`{c}`")

    if isinstance(old_df.schema[c].dataType, NumericType):
        o = old_col.cast("double")
        n = F.trim(new_col).cast("double")   # unparseable text -> null -> shows as mismatch
        same = (o.isNull() & n.isNull()) | F.coalesce(F.abs(o - n) <= 0.01, F.lit(False))
        return ~same

    return ~clean_text(old_col).eqNullSafe(clean_text(new_col))


mismatch = {c: mismatch_expr(c) for c in compare_columns}

# --- Mismatches per column, highest first -----------------------------
# One row with a count per column, then turned into one row per column (an unpivot)
counts_row = joined.agg(*[
    F.sum(F.when(mismatch[c], 1).otherwise(0)).alias(c) for c in compare_columns
])
mismatch_summary = (
    counts_row.select(F.explode(F.array(*[
        F.struct(F.lit(c).alias("column"), F.col(f"`{c}`").alias("mismatch_count"))
        for c in compare_columns
    ])).alias("r"))
    .select("r.*")
    .filter("mismatch_count > 0")
    .orderBy(F.desc("mismatch_count"))
)
display(mismatch_summary)

# --- Which columns differ, per fight -----------------------------------
mismatched_fights = (
    joined
    .withColumn(
        "mismatched_columns",
        F.filter(F.array(*[F.when(mismatch[c], F.lit(c)) for c in compare_columns]),
                 lambda x: x.isNotNull()),
    )
    .filter(F.size("mismatched_columns") > 0)
)
print(f"Fights with at least one mismatch: {mismatched_fights.count()}")

# --- Three examples per suspect column ---------------------------------
SUSPECT_COLUMNS = ["f_1_ctrl_time_sec", "f_1_r1_ctrl", "f_1_ko_odds",
                   "f_1_sub_odds", "f_1_ranking", "result", "title_fight"]

for c in [c for c in SUSPECT_COLUMNS if c in compare_columns]:
    print(f"=== {c} ===")
    display(
        joined.filter(mismatch[c])
        .select("fight_url",
                F.col(f"old.`{c}`").alias(f"old_{c}"),
                F.col(f"new.`{c}`").alias(f"new_{c}"))
        .limit(3)
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
