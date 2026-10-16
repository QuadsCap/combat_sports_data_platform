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

# ## 31_warehouse_fact_round — Silver → Gold fact table.
# 
# Builds `fact_round`: one row per fighter per round (`docs/decisions.md` 2026-10-08).
# 
# - **Reads:** `silver_layer.dbo.silv_round`, `silv_fight` (never bronze); gold dims for key checks
# - **Writes:** `gold_layer.dbo.fact_round`
# - **Run order:** after `30_warehouse_dims` (the FK checks read the dimensions)
# - **Keys:** PK = `fight_key` + `round` + `fighter_key`; FKs to `dim_fight`, `dim_fighter` (×2), `dim_event`, `dim_date`

# CELL ********************

# Why this exists: the centre of the star. One row per fighter per round, holding that fighter's
# stats in that round. Every dimension hangs off it: dim_fight (fight_key), dim_fighter
# (fighter_key + opponent_key), dim_event (event_key), dim_date (date_key).
# Built by splitting silv_round's f1/f2 pairs into two rows, one per fighter (the corner unpivot).
# age_at_fight + won are fighter-per-fight values, repeated on each round row: count fights with
# DISTINCTCOUNT(fight_key) in Power BI, not rows.

from pyspark.sql import functions as F

silv_round = spark.read.table("silver_layer.dbo.silv_round")
silv_fight = spark.read.table("silver_layer.dbo.silv_fight").select(
    "fight_id", "event_id", "event_date", "f1_fighter_id", "f2_fighter_id", "winner_fighter_id")
dob = spark.read.table("silver_layer.dbo.silv_fighter").select("fighter_id", "dob")

rounds = silv_round.join(silv_fight, "fight_id")

# Stat names without the corner prefix: f1_knockdowns -> knockdowns
STATS = [c[3:] for c in silv_round.columns if c.startswith("f1_")]

def one_corner(me, opp):
    # One fighter's view of every round: their stats, their key, their opponent's key
    return (rounds
        .join(dob, rounds[f"f{me}_fighter_id"] == dob["fighter_id"], "left")
        .select(
            F.col("fight_id").alias("fight_key"),
            "round",
            F.xxhash64(f"f{me}_fighter_id").alias("fighter_key"),
            F.xxhash64(f"f{opp}_fighter_id").alias("opponent_key"),
            F.col("event_id").alias("event_key"),
            F.date_format("event_date", "yyyyMMdd").cast("int").alias("date_key"),
            # age in whole years at the fight (null if dob unknown)
            F.floor(F.months_between("event_date", "dob") / 12).cast("int").alias("age_at_fight"),
            # 1 = this fighter won the fight; 0 = lost, draw or no contest
            (F.coalesce(F.col("winner_fighter_id") == F.col(f"f{me}_fighter_id"), F.lit(False))).cast("int").alias("won"),
            "stats_missing",
            *[F.col(f"f{me}_{s}").alias(s) for s in STATS],
        ))

fact_round = one_corner(1, 2).unionByName(one_corner(2, 1))

# --- Checks (blocking) ---
# Fabric/Delta does not enforce primary or foreign keys, so we enforce them here.
# Each assert stops the cell if the rule is broken, so a bad table is never written.
# (decisions.md 2026-10-10: key enforcement = blocking asserts in code)

n = fact_round.count()
expected = silv_round.count() * 2
print("rows:", n, "| expected:", expected)
assert n == expected, f"row count {n} != {expected} (2 rows per round)"

# PK: fight_key + round + fighter_key must be unique and not null
pk = ["fight_key", "round", "fighter_key"]
assert fact_round.select(pk).distinct().count() == n, "PK not unique"
assert fact_round.filter(" OR ".join(f"{c} IS NULL" for c in pk)).count() == 0, "PK has nulls"
assert fact_round.filter(F.col("fighter_key") == F.col("opponent_key")).count() == 0, "fighter = opponent"

# FKs: every key must exist in its dimension (left_anti = rows with no match)
fks = [
    ("fight_key",    "dim_fight",   "fight_key"),
    ("event_key",    "dim_event",   "event_key"),
    ("date_key",     "dim_date",    "date_key"),
    ("fighter_key",  "dim_fighter", "fighter_key"),
    ("opponent_key", "dim_fighter", "fighter_key"),
]
for fk, dim, pk_col in fks:
    d = spark.read.table(f"gold_layer.dbo.{dim}").select(F.col(pk_col).alias("_pk"))
    orphans = fact_round.join(d, fact_round[fk] == d["_pk"], "left_anti").count()
    print(f"{fk} -> {dim}: {orphans} orphans")
    assert orphans == 0, f"{orphans} {fk} values missing in {dim}"

# Nothing lost in the unpivot: totals must match silver (f1 + f2)
for s in ["sig_strikes_succ", "knockdowns", "ctrl_sec"]:
    silver_total = silv_round.agg(F.sum(f"f1_{s}") + F.sum(f"f2_{s}")).first()[0]
    gold_total = fact_round.agg(F.sum(s)).first()[0]
    print(f"{s}: silver {silver_total} | gold {gold_total}")
    assert silver_total == gold_total, f"{s} total changed in unpivot"

# The 23 fights without round stats are carried through, flagged
missing = fact_round.filter("stats_missing").select("fight_key").distinct().count()
print("stats_missing fights:", missing)
assert missing == 21, f"{missing} stats_missing fights, expected 21"

# won: exactly one winner per decided fight, none on draws / NC
fight_wins = fact_round.select("fight_key", "fighter_key", "won").distinct().groupBy("fight_key").agg(F.sum("won").alias("w"))
print("fights by number of winners:"); fight_wins.groupBy("w").count().orderBy("w").show()
assert fight_wins.filter("w > 1").count() == 0, "fight with two winners"

# age_at_fight: plausible range
fact_round.select(F.min("age_at_fight"), F.max("age_at_fight"), F.sum(F.col("age_at_fight").isNull().cast("int")).alias("null_rows")).show()

print("All checks passed.")

# --- Write (only reached if every assert passed) ---
fact_round.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold_layer.dbo.fact_round")
print(spark.read.table("gold_layer.dbo.fact_round").count(), "rows written")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
