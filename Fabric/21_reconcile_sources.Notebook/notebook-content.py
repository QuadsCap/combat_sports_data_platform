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
# META         },
# META         {
# META           "id": "82bd5d46-def0-4ad0-b61b-4f40cfc43ae4"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## 21_reconcile_sources — Cross-source reconciliation (silver)
# 
# Compares two sources on the fights they both cover, after both have been cleaned to the silver contract. Answers: do the sources agree on *which* fights happened, and on *what* happened in them? Disagreements are counted and explained, not resolved here — gold decides which copy it keeps (decisions.md 2026-10-22).
# 
# - **Reads:** `silver_layer.dbo.silv_fight`, `silv_round`, `silv_fighter` (silver only, no source column names)
# - **Writes:** nothing
# - **Run order:** after all `20_clean_*` notebooks; not part of the pipeline run
# - **Current pair:** kagglesk vs Cito on their 5 shared events (19 Sep → 17 Oct 2026)
# - **Match key:** `event_id` + the two fighter ids in either corner (`LEAST` / `GREATEST`) — fight ids differ per source (`cito_…` vs UFCStats), and f1/f2 are assigned independently per source
# - **Doubles as a test of entity resolution:** a fighter matched to the wrong person by name makes the pair fail to line up
# - **Key output:** matched / only-in-one-source counts, then per-column disagreement counts; written up in `docs/data-coverage.md`


# CELL ********************

# =====================================================================
# Reconciliation A: match kagglesk and Cito fights on the overlap events
# ---------------------------------------------------------------------
# What:   pairs each Cito fight with the kagglesk fight between the same two
#         fighters at the same event, whichever corner they are in.
# Why:    Cito fight_ids are 'cito_…', kagglesk's are UFCStats ids -> no shared fight key.
#         Event + both fighters is the key (decisions.md 2026-10-22). It also tests
#         yesterday's 124 name matches: a wrong match makes the pair not line up.
# Reads:  silver only (silv_fight, silv_fighter)
# Expect: ~62 matched; few or none on one side only
# =====================================================================

from pyspark.sql import functions as F

S = "silver_layer.dbo."
fight = spark.read.table(S + "silv_fight")

def with_pair(df):
    # Order-independent fighter pair. SQL: LEAST(f1, f2) AS fighter_lo, GREATEST(f1, f2) AS fighter_hi
    return (df.withColumn("fighter_lo", F.least("f1_fighter_id", "f2_fighter_id"))
              .withColumn("fighter_hi", F.greatest("f1_fighter_id", "f2_fighter_id")))

cito = with_pair(fight.filter("source = 'cito'"))
overlap_events = cito.select("event_id").distinct()

# kagglesk limited to the events Cito also has. left_semi = WHERE event_id IN (...)
ksk = with_pair(fight.filter("source = 'kagglesk'").join(overlap_events, "event_id", "left_semi"))

KEY = ["event_id", "fighter_lo", "fighter_hi"]
matched = (cito.select(*KEY, F.col("fight_id").alias("cito_fight_id"))
           .join(ksk.select(*KEY, F.col("fight_id").alias("ksk_fight_id")), KEY, "full_outer"))

print("overlap events:", overlap_events.count())
print("cito fights:", cito.count(), "| kagglesk fights on those events:", ksk.count())
print("matched (both):", matched.filter("cito_fight_id IS NOT NULL AND ksk_fight_id IS NOT NULL").count())
print("only cito:     ", matched.filter("ksk_fight_id IS NULL").count())
print("only kagglesk: ", matched.filter("cito_fight_id IS NULL").count())

# Unmatched fights with fighter names, to see why they don't line up
names = spark.read.table(S + "silv_fighter").select("fighter_id", "full_name")
unmatched = (matched.filter("cito_fight_id IS NULL OR ksk_fight_id IS NULL")
    .join(names.withColumnRenamed("fighter_id", "fighter_lo").withColumnRenamed("full_name", "name_lo"), "fighter_lo", "left")
    .join(names.withColumnRenamed("fighter_id", "fighter_hi").withColumnRenamed("full_name", "name_hi"), "fighter_hi", "left")
    .select("event_id", "cito_fight_id", "ksk_fight_id", "name_lo", "name_hi")
    .orderBy("event_id", "name_lo"))
display(unmatched)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#One didnt match. Is this fight contained in CITO or is it a data error?
display(spark.sql("""
    SELECT event_slug, ingest_ts,
           instr(lower(payload), 'nallo')    AS pos_nallo,
           instr(lower(payload), 'landwehr') AS pos_landwehr
    FROM bronze_layer.dbo.bronze_cito_event_stats
    WHERE lower(payload) LIKE '%nallo%' OR lower(payload) LIKE '%landwehr%'
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================
# Reconciliation B: do kagglesk and Cito agree on what happened?
# ---------------------------------------------------------------------
# What:   on the fights matched in cell A, counts per fight-level column how often
#         the two sources disagree, split into:
#           differ   -> both have a value, and the values differ (real disagreement)
#           one null -> only one source has a value (coverage gap, not a conflict)
# Note:   winner_fighter_id is a fighter id, so the f1/f2 order of each source doesn't matter.
# Reads:  `fight`, `cito`, `ksk`, `KEY` (cell A)
# Expect: mostly 0; anything above 0 gets explained in docs/data-coverage.md
# =====================================================================

from pyspark.sql import functions as F

COMPARE = ["outcome", "winner_fighter_id", "method", "decision_type",
           "finish_round", "finish_time_sec", "weight_class", "title_fight",
           "gender", "scheduled_rounds", "referee"]
COMPARE = [c for c in COMPARE if c in fight.columns]   # skip any column the contract doesn't have

# One row per matched fight, with both sources' values side by side (inner join = matched only)
both = cito.alias("c").join(ksk.alias("k"), KEY, "inner")
print("matched fights compared:", both.count())

def norm(col):
    # Compare as trimmed lower-case text, so 'KO/TKO' vs 'ko/tko' or 1 vs 1.0 isn't a false hit
    return F.lower(F.trim(col.cast("string")))

rows = []
for c in COMPARE:
    cv, kv = norm(F.col(f"c.{c}")), norm(F.col(f"k.{c}"))
    rows.append(both.agg(
        F.lit(c).alias("column"),
        F.sum(F.when(cv.isNotNull() & kv.isNotNull() & (cv != kv), 1).otherwise(0)).alias("differ"),
        F.sum(F.when(cv.isNull() != kv.isNull(), 1).otherwise(0)).alias("one_null"),
    ))

# Stack the one-row results into one table. SQL: SELECT ... UNION ALL SELECT ...
summary = rows[0]
for r in rows[1:]:
    summary = summary.unionByName(r)
display(summary.orderBy(F.desc("differ"), F.desc("one_null")))

# Three examples per column with a disagreement, with fighter names to look them up
names = spark.read.table(S + "silv_fighter").select("fighter_id", "full_name")
for row in summary.filter("differ > 0 OR one_null > 0").collect():
    c = row["column"]
    print(f"=== {c}: differ {row['differ']}, one null {row['one_null']} ===")
    cv, kv = norm(F.col(f"c.{c}")), norm(F.col(f"k.{c}"))
    display(both.filter(~cv.eqNullSafe(kv))
        .join(names.withColumnRenamed("fighter_id", "fighter_lo").withColumnRenamed("full_name", "name_lo"), "fighter_lo", "left")
        .join(names.withColumnRenamed("fighter_id", "fighter_hi").withColumnRenamed("full_name", "name_hi"), "fighter_hi", "left")
        .select("name_lo", "name_hi",
                F.col(f"c.{c}").alias(f"cito_{c}"), F.col(f"k.{c}").alias(f"kagglesk_{c}"))
        .limit(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("""
    SELECT f.fight_id, f.event_date, f.outcome, f.method, b.winner, b.result, b.result_details
    FROM silver_layer.dbo.silv_fight f
    JOIN bronze_layer.dbo.bronze_kagglesk_full_data_silver_v2 b
      ON b.fight_url LIKE concat('%', f.fight_id)
    WHERE f.source = 'kagglesk' AND (f.outcome = 'UNMAPPED' OR f.outcome IS NULL)
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Test if all data is as it should be: It shows the counts and the last write for every table in one go
for t in ["silv_event", "silv_fight", "silv_round", "silv_fighter"]:
    print(t)
    spark.read.table("silver_layer.dbo." + t).groupBy("source").count().show()
    spark.sql(f"DESCRIBE HISTORY silver_layer.dbo.{t}").select("timestamp", "operation").show(3, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

# Read the Silver fight table
df = spark.read.table("silver_layer.dbo.silv_fight")

# Keep only events that appear under more than one source
overlap_events = (
    df.groupBy("event_id")
      .agg(F.countDistinct("source").alias("source_count"))
      .filter(F.col("source_count") > 1)
      .select("event_id")
)

# Count fights per event and source
fight_counts = (
    df.join(overlap_events, on="event_id", how="inner")
      .groupBy("event_id", "source")
      .agg(F.count("*").alias("fight_count"))
)

# Pivot sources into side-by-side columns
comparison = (
    fight_counts
      .groupBy("event_id")
      .pivot("source", ["cito", "kagglesk"])
      .agg(F.first("fight_count"))
      .fillna(0)
)

# Display fight counts for every overlapping event
display(comparison.orderBy("event_id"))

# Check whether Cito has at least as many fights as Kaggle on every event
violations = comparison.filter(F.col("cito") < F.col("kagglesk"))

print(f"Number of events where Cito has fewer fights: {violations.count()}")
display(violations.orderBy("event_id"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Trace one fight, step 1 of 2: SILVER
# MAGIC -- Why this exists: every other check compares our layers with each other (counts, keys,
# MAGIC -- silver = gold totals). A consistent error - landed/attempted swapped, corners swapped,
# MAGIC -- rounds shifted - passes all of them. Only a comparison with an outside truth
# MAGIC -- (the ufcstats.com fight page) catches it. Fight: Lone'er Kavanagh vs Ramazan Temirov,
# MAGIC -- UFC 333, 2026-10-24 - Cito only (no other source to reconcile against), and the
# MAGIC -- fighter the apostrophe fix applies to (decisions.md 2026-10-28).
# MAGIC -- Grain: silv_round = one row per fight x round, both corners side by side (f1_ / f2_).
# MAGIC SELECT r.round,
# MAGIC        f1.full_name AS f1,                          -- corner 1, as our code assigned it
# MAGIC        r.f1_sig_strikes_succ, r.f1_sig_strikes_att, -- "16 of 40" from Cito -> 16 / 40
# MAGIC        f2.full_name AS f2,                          -- corner 2
# MAGIC        r.f2_sig_strikes_succ, r.f2_sig_strikes_att,
# MAGIC        r.f1_knockdowns, r.f2_knockdowns,
# MAGIC        r.f1_ctrl_sec,   r.f2_ctrl_sec               -- control time in seconds ("1:23" -> 83)
# MAGIC FROM silver_layer.dbo.silv_round r
# MAGIC -- the round row only knows fight_id; which fighter is f1 / f2 is stored on the fight
# MAGIC JOIN silver_layer.dbo.silv_fight sf   ON r.fight_id = sf.fight_id
# MAGIC -- the same fighter table twice under two aliases, once per corner, to turn ids into names
# MAGIC JOIN silver_layer.dbo.silv_fighter f1 ON sf.f1_fighter_id = f1.fighter_id
# MAGIC JOIN silver_layer.dbo.silv_fighter f2 ON sf.f2_fighter_id = f2.fighter_id
# MAGIC -- Cito fight ids are "cito_" + Cito's bout id, so the key itself shows the source
# MAGIC WHERE r.fight_id = 'cito_13066'
# MAGIC ORDER BY r.round

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Trace one fight, step 2 of 2: GOLD
# MAGIC -- Same fight after the unpivot. Grain: fact_round = one row per fight x round x fighter,
# MAGIC -- so every silver row above becomes TWO rows here: one with f1's stats, one with f2's.
# MAGIC -- Expect: the same numbers as silver, each on the right fighter's row; same rounds, no extras.
# MAGIC SELECT fr.round,
# MAGIC        d.full_name,
# MAGIC        d.fighter_id,          -- Kavanagh must show bb2c3c3a4662... (his UFCStats id), not cito_...:
# MAGIC                               -- proves the apostrophe fix reached gold
# MAGIC        fr.sig_strikes_succ, fr.sig_strikes_att,
# MAGIC        fr.knockdowns,
# MAGIC        fr.ctrl_sec,
# MAGIC        fr.won                 -- 1 on every round row of the winner, 0 for the opponent
# MAGIC FROM gold_layer.dbo.fact_round fr
# MAGIC -- fighter_key is a hash (xxhash64 of fighter_id) - unreadable on its own;
# MAGIC -- the dimension turns it back into a name and the original id
# MAGIC JOIN gold_layer.dbo.dim_fighter d ON fr.fighter_key = d.fighter_key
# MAGIC -- in gold the fight key is the silver fight_id, unchanged
# MAGIC WHERE fr.fight_key = 'cito_13066'
# MAGIC ORDER BY fr.round, d.full_name

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
