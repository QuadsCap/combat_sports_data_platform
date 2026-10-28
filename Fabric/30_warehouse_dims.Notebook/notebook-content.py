# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "0b43d889-642a-4866-89cb-4c3ef29320c9",
# META       "default_lakehouse_name": "gold_layer",
# META       "default_lakehouse_workspace_id": "cba38f54-3fe7-486e-a341-b3fbdb58bdc2",
# META       "known_lakehouses": [
# META         {
# META           "id": "0b43d889-642a-4866-89cb-4c3ef29320c9"
# META         },
# META         {
# META           "id": "82bd5d46-def0-4ad0-b61b-4f40cfc43ae4"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## 30_warehouse_dims — Silver → Gold dimensions
# 
# Builds the dimension tables of the gold star schema (`docs/decisions.md` 2026-10-08).
# 
# - **Reads:** `silver_layer.dbo.silv_event`, `silv_fighter`, `silv_fight` (never bronze)
# - **Writes:** `gold_layer.dbo.dim_date`, `dim_event`, `dim_fighter`, `dim_fight`
# - **Run order:** `dim_date` first; `dim_event` checks its `date_key` against it. Each cell is self-contained.
# - **Keys:** `event_key` / `fight_key` = UFCStats IDs; `fighter_key` = `xxhash64(fighter_id)`

# CELL ********************

# dim_date: one row per calendar day (gold star schema, decisions.md 2026-10-10).
# Range: earliest event_date in silver -> 2026-12-31. Generated, not read from a source.

# Why this exists: every date in the star links here via date_key, so Power BI and the model can
# group and filter by year, quarter, month or weekday without date logic in every query.
# It is generated (one row per day, no gaps), not read from a source, so even days without
# events exist. That keeps time axes continuous and lets any future fact join on date_key.

from pyspark.sql import functions as F
import datetime

start = spark.read.table("silver_layer.dbo.silv_event").agg(F.min("event_date")).first()[0]
end   = datetime.date(2026, 12, 31)

d = F.col("date")

dim_date = (
    spark.range(1)
    .select(F.explode(F.sequence(F.lit(start), F.lit(end), F.expr("interval 1 day"))).alias("date"))
    .select(
        F.date_format(d, "yyyyMMdd").cast("int").alias("date_key"),
        d,
        F.year(d).alias("year"),
        F.quarter(d).alias("quarter"),
        F.month(d).alias("month"),
        F.date_format(d, "MMMM").alias("month_name"),
        F.dayofmonth(d).alias("day_of_month"),
        (((F.dayofweek(d) + 5) % 7) + 1).alias("day_of_week"),   # Spark: 1 = Sunday -> ours: 1 = Monday
        F.date_format(d, "EEEE").alias("day_name"),
    )
)

# --- Checks ---
expected_days = (end - start).days + 1
print("range:", start, "->", end)
print("rows:", dim_date.count(), "| expected days:", expected_days)
print("date_key unique:", dim_date.select("date_key").distinct().count(), "(expect", expected_days, ")")
print("leap day 2024-02-29 present:", dim_date.filter(d == "2024-02-29").count(), "(expect 1)")
dim_date.filter(d.isin("2026-03-28", "2026-03-29")).show()   # Sat + Sun: day_of_week 6 and 7

# --- Write ---
dim_date.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_layer.dbo.dim_date")
print(spark.read.table("gold_layer.dbo.dim_date").count(), "rows written")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Why this exists: the "where and what" of each fight: event name, location, sport, promotion.
# fact_round points here via event_key, so every round can be filtered by event, country or
# promotion. event_key is the UFCStats event_id from silver (no new key, just renamed);
# date_key links the event to dim_date.
# Overlap rule (decisions.md 2026-10-26): silver holds some events from two sources; gold keeps
# one. If Cito has the event, only Cito's rows are used; otherwise the other sources' rows.

from pyspark.sql import functions as F

# era: splits the data into periods so they can be analysed separately or filtered out.
# Name -> first day of the era; each era runs until the next one starts.
# To add an era, add a line. Order does not matter; the code sorts by date.
ERAS = {
    "pre_unified": "1993-01-01",   # 1994–2000: few rules, tournaments, open weight, long/no rounds
    "unified":     "2000-11-17",   # UFC 28: first event under the unified rules (baseline uses this era)
}

era_col = F.lit(None)
for name, start in sorted(ERAS.items(), key=lambda kv: kv[1]):
    era_col = F.when(F.col("event_date") >= start, name).otherwise(era_col)

silv_event_all = spark.read.table("silver_layer.dbo.silv_event")
cito_event_ids = silv_event_all.filter("source = 'cito'").select("event_id").distinct()

def cito_wins(df):
    # Cito rows + non-Cito rows whose event Cito doesn't have.
    # left_anti = SQL "WHERE event_id NOT IN (cito events)". Also used in the dim_fight cell.
    return df.filter("source = 'cito'").unionByName(
        df.filter("source != 'cito'").join(cito_event_ids, "event_id", "left_anti"))

silv_event = cito_wins(silv_event_all)

dim_event = silv_event.select(
    F.col("event_id").alias("event_key"),
    "event_name",
    "event_date",
    F.date_format("event_date", "yyyyMMdd").cast("int").alias("date_key"),
    "event_city",
    "event_state",
    "event_country",
    "sport",
    "promotion",
    "source",
    era_col.alias("era"),
)

# --- Checks (blocking) ---
dim_date = spark.read.table("gold_layer.dbo.dim_date")
n = dim_event.count()
print("rows:", n, "| silver rows:", silv_event_all.count(), "| dropped (overlap):", silv_event_all.count() - n, "(expect 5)")
assert dim_event.select("event_key").distinct().count() == n, "event_key not unique"
assert dim_event.filter(F.col("event_key").isNull()).count() == 0, "event_key null"
assert dim_event.filter(F.col("date_key").isNull()).count() == 0, "date_key null"
assert dim_event.join(dim_date, "date_key", "left_anti").count() == 0, "date_key not in dim_date"
assert dim_event.filter(F.col("era").isNull()).count() == 0, "era null: an event predates the first era"
dim_event.groupBy("era").agg(F.count("*").alias("events"), F.min("event_date").alias("first"), F.max("event_date").alias("last")).orderBy("first").show()

# --- Write ---
dim_event.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_layer.dbo.dim_event")
print(spark.read.table("gold_layer.dbo.dim_event").count(), "rows written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# Why this exists: the "who" of every round. fact_round points here twice: fighter_key (whose
# stats are on the row) and opponent_key (who they fought). SCD Type 1: one row per fighter,
# current values only (decisions.md 2026-10-07). fighter_key is our own key, stable on re-run
# (xxhash64 of the UFCStats id), so a future source without UFCStats ids can get keys too.

from pyspark.sql import functions as F

silv_fighter = spark.read.table("silver_layer.dbo.silv_fighter")

dim_fighter = silv_fighter.select(
    F.xxhash64("fighter_id").alias("fighter_key"),
    "fighter_id",
    "full_name",
    # display_name: name + nickname, so Power BI can tell fighters with the same name apart
    F.when(F.col("nickname").isNotNull(),
           F.concat(F.col("full_name"), F.lit(' "'), F.col("nickname"), F.lit('"')))
     .otherwise(F.col("full_name")).alias("display_name"),
    "first_name",
    "last_name",
    "nickname",
    "dob",
    "height_cm",
    "reach_cm",
    "stance",
    "weight_lbs",
    "source",
)

# --- Checks ---
fighter_count = silv_fighter.count()
fighter_key_count = dim_fighter.select("fighter_key").distinct().count()
fighter_id_count = dim_fighter.select("fighter_id").distinct().count()

print("rows:", fighter_count)
print("fighter_key unique:", fighter_key_count)
print("fighter_id unique:", fighter_id_count)

assert fighter_key_count == fighter_count, "fighter_key is not unique (possible hash collision or duplicate fighter)"
assert fighter_id_count == fighter_count, "fighter_id is not unique (duplicate fighter identities)"

print("display_name null:", dim_fighter.filter(F.col("display_name").isNull()).count(), "(expect 0)")

dupes = dim_fighter.groupBy("display_name").count().filter("count > 1")
print("display_name shared by >1 fighter:", dupes.count(), "(expect 0; else Power BI slicer can't tell them apart)")
dupes.show(truncate=False)

# New fighter identities by source
new_fighters_by_source = dim_fighter.groupBy("source").count().orderBy("source")
new_fighters_by_source.show(truncate=False)

# Duplicate identity check: same person under two ids (UFCStats id + cito_<uuid>).
# Uniqueness asserts can't see this - the two ids are different. Only Cito can create it:
# Kaggle and kagglesk both use UFCStats ids, so the same person always gets the same id.
# Report only: a genuine Cito debut no other source has yet is legitimate.
cito_ids = dim_fighter.filter(F.col("fighter_id").startswith("cito_"))
print("cito_ fighter ids:", cito_ids.count(), "(expect 0; all Cito fighters matched on 2026-10-24)")

# If any: same full_name on a UFCStats-id fighter = probable missed match, not a debut
name_clash = (cito_ids.select("fighter_id", "full_name")
    .join(dim_fighter.filter(~F.col("fighter_id").startswith("cito_"))
                     .select(F.col("fighter_id").alias("ufcstats_id"), "full_name"),
          "full_name", "inner"))
print("cito_ fighters whose name exists under a UFCStats id:", name_clash.count(), "(expect 0; else probable duplicate identity)")
name_clash.show(truncate=False)

# --- Write ---
dim_fighter.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_layer.dbo.dim_fighter")
print(spark.read.table("gold_layer.dbo.dim_fighter").count(), "rows written")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Why this exists: everything that is true for the whole fight, not per round: weight class,
# method, outcome, title, referee, and the odds (benchmark only, never a model input).
# fact_round points here via fight_key. Corner-specific fields stay as f1/f2 pairs, with
# f1/f2_fighter_key telling which fighter each side is (option A, decisions.md 2026-10-08).
# Same overlap rule as dim_event: cito_wins() is defined in the dim_event cell (run that first).

from pyspark.sql import functions as F

silv_fight_all = spark.read.table("silver_layer.dbo.silv_fight")
silv_fight = cito_wins(silv_fight_all)

def fighter_key(col):
    # Same key as dim_fighter (xxhash64 of the UFCStats id). THIS IS A NULLGUARD: Spark hashes null to a
    # fixed number (42), which would make "no winner" look like a real fighter.
    return F.when(F.col(col).isNotNull(), F.xxhash64(col))

dim_fight = silv_fight.select(
    F.col("fight_id").alias("fight_key"),
    fighter_key("f1_fighter_id").alias("f1_fighter_key"),
    fighter_key("f2_fighter_id").alias("f2_fighter_key"),
    fighter_key("winner_fighter_id").alias("winner_fighter_key"),
    "outcome",
    "method",
    "decision_type",
    "method_detail",
    "weight_class",
    "gender",
    "title_fight",
    "scheduled_rounds",
    "finish_round",
    "finish_time_sec",
    "referee",
    "f1_ranking",
    "f2_ranking",
    "odds_source",
    "f1_odds_decimal",
    "f2_odds_decimal",
    "f1_bfo_best_decimal",
    "f2_bfo_best_decimal",
    "bfo_bookmaker_count",
    "f1_ko_odds",
    "f2_ko_odds",
    "f1_sub_odds",
    "f2_sub_odds",
    "source",
)

# Weight-class display order and division limits - UFC ONLY (Emil, 2026-10-14).
# Other promotions use the same names with different limits (ONE Flyweight = 135 lb).
# Proper fix: reference table keyed on promotion + weight_class (see decisions.md).
UFC_WEIGHT_CLASS_SORT = {
    "Flyweight": 1, "Bantamweight": 2, "Featherweight": 3, "Lightweight": 4,
    "Welterweight": 5, "Middleweight": 6, "Light Heavyweight": 7, "Heavyweight": 8,
    "Women's Strawweight": 9, "Women's Flyweight": 10, "Women's Bantamweight": 11, "Women's Featherweight": 12,
    "Catchweight": 13, "Open Weight": 14,
}
UFC_WEIGHT_LIMIT_LBS = {
    "Flyweight": 125, "Bantamweight": 135, "Featherweight": 145, "Lightweight": 155,
    "Welterweight": 170, "Middleweight": 185, "Light Heavyweight": 205, "Heavyweight": 265,
    "Women's Strawweight": 115, "Women's Flyweight": 125, "Women's Bantamweight": 135, "Women's Featherweight": 145,
}

def to_map(d):
    return F.create_map([F.lit(x) for pair in d.items() for x in pair])

dim_fight = (
    dim_fight
    .withColumn("weight_class_sort", to_map(UFC_WEIGHT_CLASS_SORT)[F.col("weight_class")])
    .withColumn("weight_limit_lbs", to_map(UFC_WEIGHT_LIMIT_LBS)[F.col("weight_class")])
)

# --- Checks (blocking) ---
dim_fighter = spark.read.table("gold_layer.dbo.dim_fighter")
n = dim_fight.count()
print("rows:", n, "| silver rows:", silv_fight_all.count(), "| dropped (overlap):", silv_fight_all.count() - n, "(expect 63)")
assert dim_fight.select("fight_key").distinct().count() == n, "fight_key not unique"
assert dim_fight.filter(F.col("fight_key").isNull()).count() == 0, "fight_key null"

# Overlap rule held: no event has fights from more than one source
multi = silv_fight.groupBy("event_id").agg(F.countDistinct("source").alias("n_src")).filter("n_src > 1").count()
assert multi == 0, f"{multi} events with fights from more than one source"

assert dim_fight.join(dim_fighter, dim_fight.f1_fighter_key == dim_fighter.fighter_key, "left_anti").count() == 0, "f1_fighter_key not in dim_fighter"
assert dim_fight.join(dim_fighter, dim_fight.f2_fighter_key == dim_fighter.fighter_key, "left_anti").count() == 0, "f2_fighter_key not in dim_fighter"
print("winner_fighter_key null (draws + NC):", dim_fight.filter(F.col("winner_fighter_key").isNull()).count())
assert dim_fight.filter(F.col("winner_fighter_key").isNotNull()
      & (F.col("winner_fighter_key") != F.col("f1_fighter_key")) & (F.col("winner_fighter_key") != F.col("f2_fighter_key"))).count() == 0, "winner not one of the two fighters"

# Guard: the weight-class maps above are UFC-only
non_ufc = silv_event.filter(F.col("promotion") != "UFC").count()
assert non_ufc == 0, f"{non_ufc} non-UFC events - weight-class maps are UFC-only, build the promotion-keyed reference first"

assert dim_fight.filter(F.col("weight_class_sort").isNull()).count() == 0, "weight_class without sort number"
assert dim_fight.filter(F.col("weight_limit_lbs").isNull() & ~F.col("weight_class").isin("Catchweight", "Open Weight")).count() == 0, "weight_class without lbs limit"

# --- Write ---
dim_fight.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_layer.dbo.dim_fight")
print(spark.read.table("gold_layer.dbo.dim_fight").count(), "rows written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
