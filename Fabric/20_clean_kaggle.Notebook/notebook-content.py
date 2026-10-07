# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "82bd5d46-def0-4ad0-b61b-4f40cfc43ae4",
# META       "default_lakehouse_name": "silver_layer",
# META       "default_lakehouse_workspace_id": "cba38f54-3fe7-486e-a341-b3fbdb58bdc2",
# META       "known_lakehouses": [
# META         {
# META           "id": "5f48b752-8972-44bf-8312-1fa1d43a6b76"
# META         },
# META         {
# META           "id": "82bd5d46-def0-4ad0-b61b-4f40cfc43ae4"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import functions as F

#Construct natural keys from the urls of figthr, event and fighter_1/_2 columns in the kaggle dataset

df = spark.read.table("bronze_layer.dbo.bronze_kaggle_full_data_silver_plus")

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
df = spark.read.table("bronze_layer.dbo.bronze_kaggle_full_data_silver_plus")

df = df.withColumn("event_date", F.to_date("event_date"))
df.select(F.min("event_date"), F.max("event_date")).show()

df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

bronze = bronze = spark.read.table("bronze_layer.dbo.bronze_kaggle_full_data_silver_plus")
print(len(bronze.columns))
for c in sorted(bronze.columns):
    print(c)
#Count columns of the dataset.
len(bronze.columns)  

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Kaggle name -> contract name (docs/silver-contract.md).
# The select is also the whitelist: columns not listed here never reach silver.
# Kaggle names must not appear anywhere below this cell.
# Why : There is 2 prog reasoning behind this, it functions as a whitelist, to drop columns that are not necessary, and the dictionary functions as translation if we are ever getting data from -
# somewhere that is not cleaned.
# Kaggle name -> contract name (docs/silver-contract.md).
# Two reasons: the select is a whitelist (columns not listed never reach silver),
# and the dicts are the translation layer if another source is added later.
# Kaggle names must not appear anywhere below this cell.

from pyspark.sql import functions as F

df = spark.read.table("bronze_layer.dbo.bronze_kaggle_full_data_silver_plus")

# Natural keys parsed from the UFCStats URLs
df = (
    df
    .withColumn("fight_id",      F.regexp_extract("fight_url",       r"/fight-details/([0-9a-f]+)$", 1))
    .withColumn("event_id",      F.regexp_extract("event_url",       r"/event-details/([0-9a-f]+)$", 1))
    .withColumn("f1_fighter_id", F.regexp_extract("f_1_fighter_url", r"/fighter-details/([0-9a-f]+)$", 1))
    .withColumn("f2_fighter_id", F.regexp_extract("f_2_fighter_url", r"/fighter-details/([0-9a-f]+)$", 1))
)

RENAME_EVENT = {
    # keys
    "event_id":       "event_id",
    # attributes
    "event_name":     "event_name",
    "event_date":     "event_date",
    "event_city":     "event_city",
    "event_state":    "event_state",
    "event_country":  "event_country",
}

RENAME_FIGHT = {
    # keys
    "fight_id":       "fight_id",
    "event_id":       "event_id",
    "f1_fighter_id":  "f1_fighter_id",
    "f2_fighter_id":  "f2_fighter_id",

    # fight attributes
    "weight_class":   "weight_class",       # normalised later
    "gender":         "gender",
    "title_fight":    "title_fight",
    "num_rounds":     "scheduled_rounds",   # 0 -> null later
    "referee":        "referee",

    # outcome
    "result":         "result_raw",         # working column -> method, then dropped
    "result_details": "method_detail",
    "finish_round":   "finish_round",
    "finish_time":    "finish_time_raw",    # working column -> finish_time_sec, then dropped
    "winner":         "winner_name",        # working column -> winner_fighter_id, then dropped
    "f_1_name":       "f1_name",            # working column, dropped
    "f_2_name":       "f2_name",            # working column, dropped

    # ranking at this fight
    "f_1_ranking":    "f1_ranking",
    "f_2_ranking":    "f2_ranking",

    # odds
    "f_1_odds_legacy":      "f1_odds_decimal",
    "f_2_odds_legacy":      "f2_odds_decimal",
    "odds_source":          "odds_source",
    "f_1_bfo_best_decimal": "f1_bfo_best_decimal",
    "f_2_bfo_best_decimal": "f2_bfo_best_decimal",
    "bfo_bookmaker_count":  "bfo_bookmaker_count",
    "f_1_ko_odds":          "f1_ko_odds",
    "f_2_ko_odds":          "f2_ko_odds",
    "f_1_sub_odds":         "f1_sub_odds",
    "f_2_sub_odds":         "f2_sub_odds",
}

# Fighter attributes, identical for both corners: Kaggle suffix -> contract name
FIGHTER_ATTRS = {
    "name":                   "full_name",
    "fighter_f_name":         "first_name",
    "fighter_l_name":         "last_name",
    "fighter_nickname":       "nickname",
    "fighter_dob":            "dob",
    "fighter_height_cm":      "height_cm",
    "fighter_reach_cm":       "reach_cm",
    "fighter_stance":         "stance",
    "fighter_weight_lbs":     "weight_lbs",
}

def rename_corner(n):
    cols = [F.col(f"f{n}_fighter_id").alias("fighter_id")]
    cols += [F.col(f"f_{n}_{k}").alias(v) for k, v in FIGHTER_ATTRS.items()]
    return df.select(cols)

event      = df.select([F.col(k).alias(v) for k, v in RENAME_EVENT.items()])
fight      = df.select([F.col(k).alias(v) for k, v in RENAME_FIGHT.items()])
fighter_f1 = rename_corner(1)
fighter_f2 = rename_corner(2)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#We are checking if the renaming in the above cell actually worked in the new data frame.
print("event cols:",   len(event.columns),      "(expect 6)")
print("fight cols:",   len(fight.columns),      "(expect 30)")
print("fighter cols:", len(fighter_f1.columns), "(expect 10)")
print("fight rows:",   fight.count(),           "(expect 8555)")
print("f1 rows:",      fighter_f1.count(), "| f2 rows:", fighter_f2.count(), "(expect 8555 each)")
print("events:",       event.distinct().count(), "(expect 774)")

# Totals-only fights: round detail missing, fight totals present
print("totals-only:", df.filter(F.col("f_1_r1_knockdowns").isNull() & F.col("f_1_knockdowns").isNotNull()).count(), "(expect ~21)")

fight.limit(5).show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(df.filter(F.col("f_1_r1_sig_strikes_att").isNull()).count(), "fights without R1")
print(df.filter(F.col("f_1_r1_sig_strikes_att").isNull() & F.col("f_1_sig_strikes_att").isNotNull()).count(), "of those with totals")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Collapse literal "\n" and real whitespace runs into one space, then trim the ends
WS = r"(\\n|\s)+"

fight = (
    fight
    .withColumn("result_raw",    F.trim(F.regexp_replace(F.col("result_raw"),    WS, " ")))
    .withColumn("method_detail", F.trim(F.regexp_replace(F.col("method_detail"), WS, " ")))
)

event = event.distinct()
print("events:", event.count(), "(expect 774)")

print("literal \\n left:", fight.filter(F.col("result_raw").contains("\\n") | F.col("method_detail").contains("\\n")).count(), "(expect 0)")

fight.groupBy("result_raw").count().orderBy(F.desc("count")).show(50, truncate=False)
fight.groupBy("method_detail").count().orderBy(F.desc("count")).show(30, truncate=60)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. What's in method_detail when the subtype sits in result_raw?
(fight.filter(F.col("result_raw").contains(" - "))
      .groupBy("result_raw", "method_detail").count()
      .orderBy("result_raw", F.desc("count")).show(30, truncate=60))

# 2. Winner that matches neither fighter, by result
(fight.filter(~(F.col("winner_name") == F.col("f1_name")) & ~(F.col("winner_name") == F.col("f2_name")) | F.col("winner_name").isNull())
      .groupBy("result_raw", "winner_name").count()
      .orderBy(F.desc("count")).show(30, truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(fight.filter(F.col("winner_name") == "No Contest")
      .select("fight_id", "result_raw", "method_detail", "f1_name", "f2_name")
      .show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Derived fight columns (decisions.md 2026-10-06; docs/silver-contract.md)
# We are making sure that the different types of data are being sorted correctly throughout the columns (Decisions split up into una/split/major etc)

from pyspark.sql import functions as F

r   = F.col("result_raw")
det = F.col("method_detail")
win = F.col("winner_name")

# method: 5 values; anything else is flagged, never silently nulled
fight = (fight.withColumn("method",
        F.when(r.startswith("Decision"), "Decision")
         .when((r == "KO/TKO") | r.startswith("TKO"), "KO/TKO")
         .when(r == "Submission", "Submission")
         .when(r == "DQ", "DQ")
         .when(r == "Could Not Continue", "Could Not Continue")
         .otherwise("UNMAPPED"))

    # decision_type: suffix of result ("Decision - Split") if present, else result_details
    .withColumn("decision_type",
        F.when(F.col("method") != "Decision", None)
         .when(r.contains(" - "), F.regexp_extract(r, r"^Decision - (\w+)$", 1))
         .when(det.isin("Unanimous", "Split", "Majority"), det))

    # outcome: Kaggle labels both draws and NCs "No Contest"; method tells them apart
    .withColumn("outcome",
        F.when((win == F.col("f1_name")) | (win == F.col("f2_name")), "win")
         .when((win == "No Contest") & (F.col("method") == "Decision"), "draw")
         .when((win == "No Contest") & (F.col("method") == "Could Not Continue"), "no_contest")
         .otherwise("UNMAPPED"))

    # winner_fighter_id: name match within the row -> that corner's id
    .withColumn("winner_fighter_id",
        F.when(F.col("outcome") != "win", None)
         .when(win == F.col("f1_name"), F.col("f1_fighter_id"))
         .when(win == F.col("f2_name"), F.col("f2_fighter_id")))

    # finish_time_sec: "m:ss" -> seconds into the finish round
    .withColumn("finish_time_sec",
        F.split(F.col("finish_time_raw"), ":").getItem(0).cast("int") * 60
        + F.split(F.col("finish_time_raw"), ":").getItem(1).cast("int"))

    # scheduled_rounds: 0 -> null
    .withColumn("scheduled_rounds",
        F.when(F.col("scheduled_rounds") == 0, None)
         .otherwise(F.col("scheduled_rounds").cast("int")))
)


#We print the above results to verify if the data cleaning works as intended.
print("method:");        fight.groupBy("method").count().orderBy(F.desc("count")).show()
print("decision_type:"); fight.groupBy("method", "decision_type").count().orderBy("method").show()
print("outcome:");       fight.groupBy("outcome").count().show()

print("win without winner_fighter_id:", fight.filter((F.col("outcome") == "win") & F.col("winner_fighter_id").isNull()).count(), "(expect 0)")
print("same name both corners:",       fight.filter(F.col("f1_name") == F.col("f2_name")).count(), "(expect 0; else winner match is ambiguous)")
print("finish_time_sec null:",         fight.filter(F.col("finish_time_sec").isNull()).count(), "(expect 0)")
print("finish_time_sec range:");       fight.select(F.min("finish_time_sec"), F.max("finish_time_sec")).show()
print("scheduled_rounds null:",        fight.filter(F.col("scheduled_rounds").isNull()).count(), "(expect 31)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 0:00 finishes and finishes over 5 min (event is now distinct, so no fan-out)
(fight.filter((F.col("finish_time_sec") == 0) | (F.col("finish_time_sec") > 300))
      .join(event.select("event_id", "event_date"), "event_id")
      .select("event_date", "f1_name", "f2_name", "method", "finish_round", "finish_time_raw", "scheduled_rounds")
      .orderBy("event_date").show(100, truncate=False))

# Weight class variants
fight.groupBy("weight_class", "gender").count().orderBy("weight_class").show(100, truncate=False)

# Check for titlefights
fight.groupBy("title_fight").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

wc = F.col("weight_class")

fight = (
    fight
    # title in the weight-class text -> title_fight true (before the text is stripped)
    .withColumn("title_fight", F.col("title_fight") | F.coalesce(wc.contains("Title"), F.lit(False)))
    .withColumn("weight_class",
        F.when(wc.isNull() | wc.isin("NULL", "Nieznana"), "Unknown") 
         .when(wc.contains("Tournament"), "Open Weight")
         .when(wc == "Catch Weight", "Catchweight")
         .otherwise(F.trim(F.regexp_replace(wc, r"^UFC |Interim |Women's | Title$", ""))))
)

fight.groupBy("weight_class").count().orderBy(F.desc("count")).show(30, truncate=False)
fight.groupBy("title_fight").count().show()

print("real null:",   fight.filter(F.col("weight_class").isNull()).count())
print("text 'NULL':", fight.filter(F.col("weight_class") == "NULL").count())



# Junk checks (point 8)
print("same fighter both corners:", fight.filter(F.col("f1_fighter_id") == F.col("f2_fighter_id")).count(), "(expect 0)")
print("null finish_round:",         fight.filter(F.col("finish_round").isNull()).count(), "(expect 0)")
print("duplicate fight_id:",        fight.count() - fight.select("fight_id").distinct().count(), "(expect 0)")
print("decision at 0:00:",          fight.filter((F.col("method") == "Decision") & (F.col("finish_time_sec") == 0)).count(), "(expect 1, kept)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Final shape + write: contract column order and types (docs/silver-contract.md)

# Text "NULL" / "" -> real null in every string column (found in weight_class; applied generally)
def blank_to_null(d):
    for c, t in d.dtypes:
        if t == "string":
            d = d.withColumn(c, F.when(F.trim(F.col(c)).isin("", "NULL"), None).otherwise(F.col(c)))
    return d

# --- silv_event ---
silv_event = (
    blank_to_null(event)
    .distinct()
    .select(
        F.col("event_id"),
        F.col("event_name"),
        F.to_date("event_date").alias("event_date"),
        F.col("event_city"),
        F.col("event_state"),
        F.col("event_country"),
        F.lit("MMA").alias("sport"),
        F.lit("UFC").alias("promotion"),
        F.lit("kaggle").alias("source"),
    )
)

# --- silv_fight --- (working columns result_raw, finish_time_raw, winner_name, f1/f2_name dropped by the select)
silv_fight = (
    blank_to_null(fight)
    .join(silv_event.select("event_id", "event_date"), "event_id", "left")
    .select(
        F.col("fight_id"),
        F.col("event_id"),
        F.col("event_date"),
        F.col("f1_fighter_id"),
        F.col("f2_fighter_id"),
        F.col("outcome"),
        F.col("winner_fighter_id"),
        F.col("weight_class"),
        F.col("gender"),
        F.col("title_fight").cast("boolean"),
        F.col("scheduled_rounds").cast("int"),
        F.col("referee"),
        F.col("method"),
        F.col("decision_type"),
        F.col("method_detail"),
        F.col("finish_round").cast("int"),
        F.col("finish_time_sec").cast("int"),
        F.col("f1_ranking").cast("int"),
        F.col("f2_ranking").cast("int"),
        F.col("f1_odds_decimal").cast("double"),
        F.col("f2_odds_decimal").cast("double"),
        F.col("odds_source"),
        F.col("f1_bfo_best_decimal").cast("double"),
        F.col("f2_bfo_best_decimal").cast("double"),
        F.col("bfo_bookmaker_count").cast("int"),
        F.col("f1_ko_odds").cast("double"),
        F.col("f2_ko_odds").cast("double"),
        F.col("f1_sub_odds").cast("double"),
        F.col("f2_sub_odds").cast("double"),
        F.lit("kaggle").alias("source"),
    )
)

# --- Checks before writing ---
print("silv_event rows:", silv_event.count(), "(expect 774)")
print("silv_fight rows:", silv_fight.count(), "(expect 8555)")
print("event_id unique:", silv_event.select("event_id").distinct().count(), "(expect 774)")

NOT_NULL_FIGHT = ["fight_id", "event_id", "event_date", "f1_fighter_id", "f2_fighter_id", "outcome",
                  "weight_class", "gender", "title_fight", "method", "finish_round", "finish_time_sec", "source"]
NOT_NULL_EVENT = ["event_id", "event_name", "event_date", "sport", "promotion", "source"]
for c in NOT_NULL_FIGHT:
    n = silv_fight.filter(F.col(c).isNull()).count()
    if n: print("NULL in non-nullable silv_fight.", c, ":", n)
for c in NOT_NULL_EVENT:
    n = silv_event.filter(F.col(c).isNull()).count()
    if n: print("NULL in non-nullable silv_event.", c, ":", n)

# Did any cast silently null values? (ranking type was still open in the contract)
for c in ["f1_ranking", "f1_odds_decimal", "f1_ko_odds", "bfo_bookmaker_count"]:
    print(c, "non-null before:", fight.filter(F.col(c).isNotNull()).count(),
             "after:", silv_fight.filter(F.col(c).isNotNull()).count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
# We check the unknown fights - when and where?

(spark.read.table("bronze_layer.dbo.bronze_kaggle_full_data_silver_plus")
      .filter(F.col("weight_class").isNull() | F.col("weight_class").isin("NULL", "Nieznana"))
      .select("fight_url", "event_date", "event_name", "f_1_name", "f_2_name", "weight_class")
      .orderBy("event_date")
      .show(truncate=False))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Manual weight-class fixes, verified on ufcstats.com (Emil, 2026-10-07).
# Source had no class ("NULL" / "Nieznana"); keyed on fight_id.
WEIGHT_CLASS_FIX = {
    "6ca94b35719eb300": "Open Weight",   # Severn vs Beneteau, UFC 5, 1995-04-07
    "d62aec55bc142346": "Open Weight",   # Shamrock vs Severn, UFC 6, 1995-07-14
    "66c029f49d3c5da6": "Open Weight",   # Taktarov vs Abbott, UFC 6, 1995-07-14
    "e7ca291dd8f5661b": "Open Weight",   # Ruas vs Varelans, UFC 7, 1995-09-08
    "524b49a676498c6d": "Open Weight",   # Severn vs Taktarov, Ultimate Ultimate '95
    "16b4a0b06427f1ac": "Open Weight",   # Shamrock vs Leopoldo, UFC 8, 1996-02-16
    "8d2e99599124a16f": "Open Weight",   # Frye vs Goodridge, UFC 8, 1996-02-16
    "6a060498e60756af": "Open Weight",   # Severn vs Shamrock, UFC 9, 1996-05-17
    "6397fba14ce7f674": "Open Weight",   # Coleman vs Frye, UFC 10, 1996-07-12
    "d595d2c36ddba8ee": "Open Weight",   # Frye vs Abbott, Ultimate Ultimate '96
    "36ec204f47e4d613": "Catchweight",   # Musayev vs Orolbai, 2025-06-21 (165 lb)
}

fix_map = F.create_map([F.lit(x) for pair in WEIGHT_CLASS_FIX.items() for x in pair])
fight = fight.withColumn("weight_class", F.coalesce(fix_map[F.col("fight_id")], F.col("weight_class")))

print("fixes matched:", fight.filter(F.col("fight_id").isin(list(WEIGHT_CLASS_FIX))).count(), "(expect 11)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silv_event.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_layer.dbo.silv_event")
silv_fight.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver_layer.dbo.silv_fight")

print(spark.read.table("silver_layer.dbo.silv_event").count(), "/", spark.read.table("silver_layer.dbo.silv_fight").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
