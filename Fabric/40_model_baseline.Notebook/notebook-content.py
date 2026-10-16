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

# ## 40_model_baseline — Phase 1 baseline model
# 
# A simple, interpretable logistic regression that predicts **"did fighter A win?"** from the difference between the two fighters. Its test score is **the floor the Phase 3 model must beat** (`docs/decisions.md` 2026-10-10).
# 
# - **Reads:** `gold_layer.dbo.dim_fight`, `dim_event`, `dim_fighter`, `fact_round` (gold only)
# - **Writes:** `gold_layer.dbo.model_baseline_predictions`, `model_baseline_metrics` (for Power BI)
# - **Run order:** `30_warehouse_fact_round to 31_warehouse_fact_round.`
# - **Features (A − B):** prior wins, prior losses, layoff, debut, age, reach, height. Only fights *before* the predicted fight are used (no leakage)
# - **Scope:** unified-rules era only (from UFC 28, 2000-11-17); draws / no contests excluded
# - **A vs B:** assigned by a stable hash of `fight_key`, not by corner (corner 1 wins 64 %)
# - **Split:** fit on fights before 2023-01-01, test on 2023-01-01 → 2026-05-16
# - **Metrics:** accuracy, log loss, Brier, against the naive 50 % guess
# - **Deliberately simple:** no tuning, no Elo, no feature engineering beyond arithmetic


# CELL ********************

# Why this exists: the Phase 1 baseline. A simple, interpretable logistic regression that predicts
# "did fighter A win?" from the difference between the two fighters (record, age, layoff, reach,
# height). Its test score is the floor the Phase 3 model must beat (decisions.md 2026-10-10).
# Every feature uses only fights BEFORE the fight being predicted: no leakage.

import zlib
import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, log_loss, brier_score_loss

TEST_START = pd.Timestamp("2023-01-01")

# --- 1. Load gold into pandas (small: ~8.5k fights) ---
fight_event = spark.read.table("gold_layer.dbo.fact_round").select("fight_key", "event_key").distinct()
fights = (spark.read.table("gold_layer.dbo.dim_fight")
          .select("fight_key", "f1_fighter_key", "f2_fighter_key", "winner_fighter_key", "outcome")
          .join(fight_event, "fight_key")
          .join(spark.read.table("gold_layer.dbo.dim_event").select("event_key", "event_date", "era"), "event_key")
          .toPandas())
fighters = spark.read.table("gold_layer.dbo.dim_fighter").select("fighter_key", "dob", "reach_cm", "height_cm").toPandas()

fights["event_date"] = pd.to_datetime(fights["event_date"])
fighters["dob"] = pd.to_datetime(fighters["dob"])

# --- 2. Each fighter's history: one row per fighter per fight, in date order ---
long = pd.concat([
    fights.assign(fighter_key=fights["f1_fighter_key"]),
    fights.assign(fighter_key=fights["f2_fighter_key"]),
])[["fight_key", "event_date", "fighter_key", "winner_fighter_key", "outcome"]]
long["won"]  = (long["winner_fighter_key"] == long["fighter_key"]).astype(int)
long["lost"] = ((long["outcome"] == "win") & (long["won"] == 0)).astype(int)
long = long.sort_values(["fighter_key", "event_date", "fight_key"])

g = long.groupby("fighter_key")
long["prior_wins"]   = g["won"].cumsum() - long["won"]       # wins BEFORE this fight
long["prior_losses"] = g["lost"].cumsum() - long["lost"]     # losses BEFORE this fight
long["layoff_days"]  = (long["event_date"] - g["event_date"].shift(1)).dt.days
long["debut"]        = long["layoff_days"].isna().astype(int)
long["layoff_days"]  = long["layoff_days"].fillna(0)        # debut: the debut flag carries it

long = long.merge(fighters, on="fighter_key", how="left")
long["age"] = (long["event_date"] - long["dob"]).dt.days / 365.25
hist = long.set_index(["fight_key", "fighter_key"])

# --- 3. One row per fight: A vs B, A chosen by a stable hash (corner 1 wins 64%, so not f1) ---
fights = fights[(fights["outcome"] == "win") & (fights["era"] == "unified")].copy()
a_is_f1 = fights["fight_key"].map(lambda k: zlib.crc32(k.encode()) % 2 == 0)
fights["a_key"] = np.where(a_is_f1, fights["f1_fighter_key"], fights["f2_fighter_key"])
fights["b_key"] = np.where(a_is_f1, fights["f2_fighter_key"], fights["f1_fighter_key"])
fights["y"] = (fights["winner_fighter_key"] == fights["a_key"]).astype(int)

def side(key_col):
    idx = pd.MultiIndex.from_arrays([fights["fight_key"], fights[key_col]])
    return hist.loc[idx].reset_index(drop=True)

A, B = side("a_key"), side("b_key")

X = pd.DataFrame({
    "d_wins":   A["prior_wins"].values   - B["prior_wins"].values,
    "d_losses": A["prior_losses"].values - B["prior_losses"].values,
    "d_layoff": A["layoff_days"].values  - B["layoff_days"].values,
    "d_debut":  A["debut"].values        - B["debut"].values,
    "d_age":    A["age"].values          - B["age"].values,
    "d_reach":  A["reach_cm"].values     - B["reach_cm"].values,
    "d_height": A["height_cm"].values    - B["height_cm"].values,
}, index=fights.index)

# Missing dob/reach/height: fill the difference with 0 (= "no difference"). No missing flags:
# missingness is a scrape artefact (recent fighters' profiles were empty in the Kaggle scrape),
# not a fighter property, so a flag learns the wrong thing (decisions.md 2026-10-11).
for name in ["age", "reach", "height"]:
    X["d_" + name] = X["d_" + name].fillna(0)
y = fights["y"]
is_test = fights["event_date"] >= TEST_START

# --- 4. Fit on the past, test on the future ---
model = make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000))   # no tuning
model.fit(X[~is_test], y[~is_test])
p = model.predict_proba(X)[:, 1]

# --- 5. Metrics vs. the naive 50% guess ---
rows = []
for name, m in [("train", ~is_test), ("test", is_test)]:
    rows.append({
        "split": name,
        "fights": int(m.sum()),
        "a_win_share": round(y[m].mean(), 3),
        "accuracy": round(accuracy_score(y[m], p[m] > 0.5), 4),
        "log_loss": round(log_loss(y[m], p[m]), 4),
        "brier": round(brier_score_loss(y[m], p[m]), 4),
        "naive_log_loss": round(log_loss(y[m], np.full(m.sum(), 0.5)), 4),
    })
metrics = pd.DataFrame(rows)
print(metrics.to_string(index=False))

# Weights: missing flags first, then the real features, each group sorted by weight
coefs = pd.Series(model[-1].coef_[0], index=X.columns)
is_flag = coefs.index.str.startswith("missing_")
coefs = pd.concat([coefs[is_flag].sort_values(), coefs[~is_flag].sort_values()])
print("\nWeights (standardised; + = helps A win):")
print(coefs.round(3).to_string())

# --- 6. Write to gold for Power BI ---
preds = pd.DataFrame({
    "fight_key": fights["fight_key"],
    "event_date": fights["event_date"].dt.date,
    "a_fighter_key": fights["a_key"],
    "b_fighter_key": fights["b_key"],
    "p_a_win": p,
    "a_won": y,
    "split": np.where(is_test, "test", "train"),
})
spark.createDataFrame(preds).write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("gold_layer.dbo.model_baseline_predictions")
spark.createDataFrame(metrics).write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("gold_layer.dbo.model_baseline_metrics")
print("\nwritten:", len(preds), "predictions")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Diagnose test log loss: which predictions are extreme, and which fights cost the most?
# Log loss punishes confident wrong answers hard; this finds them. Diagnosis, not tuning.
t = is_test.values
pt, yt = p[t], y[t].values
loss = -(yt * np.log(pt) + (1 - yt) * np.log(1 - pt))

print("test p range:", pt.min().round(4), "->", pt.max().round(4))
print("predictions < 2% or > 98%:", ((pt < 0.02) | (pt > 0.98)).sum())

worst = X[t].assign(p_a=pt, a_won=yt, loss=loss, date=fights["event_date"][t].values).nlargest(10, "loss")
print(worst.round(2).to_string())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extreme predictions (< 2% or > 98%) in the test set, with names and links for manual checking.
# Tripwire: 87 extremes with missing flags (decisions.md 2026-10-11); 0 after the flags were removed.
# If this ever lists rows again, check them before trusting the metrics.

dimf = spark.read.table("gold_layer.dbo.dim_fighter").select(
    "fighter_key", "display_name", "dob", "reach_cm", "height_cm").toPandas()
ev = spark.read.table("gold_layer.dbo.dim_event").select("event_key", "event_name").toPandas()

ext = fights[t].assign(p_a=pt, a_won=yt)
ext = ext[(ext["p_a"] < 0.02) | (ext["p_a"] > 0.98)]

ext = (ext
    .merge(dimf.add_prefix("a_"), left_on="a_key", right_on="a_fighter_key", how="left")
    .merge(dimf.add_prefix("b_"), left_on="b_key", right_on="b_fighter_key", how="left")
    .merge(ev, on="event_key", how="left"))

ext["winner"] = np.where(ext["a_won"] == 1, ext["a_display_name"], ext["b_display_name"])
ext["fight_url"] = "http://ufcstats.com/fight-details/" + ext["fight_key"]

cols = ["event_date", "event_name", "a_display_name", "a_dob", "a_reach_cm", "a_height_cm",
        "b_display_name", "b_dob", "b_reach_cm", "b_height_cm", "p_a", "winner", "fight_url"]
out = ext[cols].sort_values("event_date")
out["p_a"] = out["p_a"].round(3)

print(len(out), "extreme predictions")
if len(out) > 0:
    display(spark.createDataFrame(out.astype(str)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Does missing reach mean something different over time? Share missing + win rate when missing, by year.
# 
# Cell here is deleted, as it interfered with "Run All"

# MARKDOWN ********************

# ## Calibration: does "70 %" really mean 70 %?
# 
# Calibration checks whether the model's probabilities can be taken at face value. One fight can't
# answer that (it either happened or it didn't), so test fights are grouped by what the model predicted.
# 
# **Bins** are probability bands, like the age bands on the dashboard. Each test fight goes into one of
# 10 bands by its `p_a_win`: bin 0 = 0.0–0.1, bin 1 = 0.1–0.2, …, bin 9 = 0.9–1.0.
# `floor(p × 10)` finds the band (0.67 → 6.7 → bin 6); p = 1.0 is clipped into bin 9.
# 
# **Per bin:**
# - `mean_predicted`: average `p_a_win`, i.e. what the model said
# - `actual_win_rate`: average `a_won` (0/1), i.e. what happened
# - `fights`: how many test fights are in the bin
# 
# **Reading the chart** (x = mean_predicted, y = actual_win_rate):
# - on the diagonal → well calibrated
# - below the diagonal at the high end → overconfident (says 80 %, wins 65 %)
# - bins with few fights are noise and are flagged, not trusted
# 
# Test split only (2023-01-01 → 2026-05-16). Written to `gold_layer.dbo.model_baseline_calibration`.


# CELL ********************

# Calibration: test-split predictions binned into 10 probability bins -> gold for Power BI
import numpy as np

cal = (spark.read.table("gold_layer.dbo.model_baseline_predictions")
       .filter("split = 'test'")
       .select("p_a_win", "a_won")
       .toPandas())

# bin 0 = [0.0, 0.1), ..., bin 9 = [0.9, 1.0]; clip so p = 1.0 lands in bin 9
cal["bin"] = np.clip(np.floor(cal["p_a_win"] * 10).astype(int), 0, 9)

calib = (cal.groupby("bin")
         .agg(mean_predicted=("p_a_win", "mean"),
              actual_win_rate=("a_won", "mean"),
              fights=("a_won", "size"))
         .reset_index())
calib["bin_lower"] = calib["bin"] / 10
calib["bin_upper"] = (calib["bin"] + 1) / 10

# checks before the write
assert calib["bin"].is_unique
assert calib["fights"].sum() == len(cal), "fights lost in binning"
print(f"test fights: {len(cal)}")   # expect 1,735
print(calib)

(spark.createDataFrame(calib)
 .write.mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable("gold_layer.dbo.model_baseline_calibration"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
