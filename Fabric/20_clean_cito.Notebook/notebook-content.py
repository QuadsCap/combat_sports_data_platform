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

# ## 20_clean_cito — Bronze → Silver (Cito source)
# 
# Cleans the raw Cito JSON into the source-agnostic silver tables defined in `docs/silver-contract.md`. The **only** notebook allowed to know Cito field names: everything is renamed to silver names here, and nothing downstream sees Cito's shape.
# 
# - **Reads:** `bronze_layer.dbo.bronze_cito_event_stats` (bronze only)
# - **Writes:** `silver_layer.dbo.silv_event`, `silv_fight`, `silv_round`, `silv_fighter` (rows with `source = 'cito'`)
# - **Run order:** after `10_ingest_cito`. Inside the notebook: latest snapshot per event → parse `payload` (JSON string → struct) → explode bouts → explode rounds → parse + pivot → rename to silver → checks
# - **Snapshots:** Cito updates finished events, so bronze holds several snapshots per event; only the latest `ingest_ts` per `event_slug` is used
# - **Keys:** `event_id` = Cito event `dataId` (UFCStats event ID); `fight_id` = `cito_` + bout `id`; `fighter_id` via `silv_fighter_xref` (Cito UUID → UFCStats ID), `cito_<uuid>` for unmatched debuts
# - **Shape changes:** JSON flattened by grain (event → bout → round); round stats from one row per fighter × round (red/blue `corner`) pivoted to one row per fight × round with `f1_` / `f2_` columns
# - **Parsing:** strike strings `"9 of 37"` → landed / attempted integers; `controlTime` `"m:ss"` → seconds
# - **Checks:** row counts before/after each explode (`explode_outer`, nothing dropped silently), referential integrity, round stats (succ ≤ att, no negatives)


# CELL ********************

from pyspark.sql import functions as F
from pyspark.sql.window import Window
import datetime

# --- Load scope ------------------------------------------------------------------
# Incremental: only events that can have changed since the last load are processed.
# Cito's free tier re-sends the last 30 days on every call, so only events inside
# that window can get a new snapshot (late stats, corrections). Older events are final.
# None = full refresh: every event in bronze. Use it after a MANUAL_MAP / NAME_FIX
# change, or when cell 9 says "FULL REFRESH NEEDED".
LOOKBACK_DAYS = 35   # 30-day API window + 5 days margin

# Read the bronze table containing historical API responses.
# Error responses (429, 500 ...) have no data: a failed call must not replace a good snapshot.
bronze = spark.read.table("bronze_cito_event_stats").filter("http_status = 200")

# Keep only the latest API response for each event.
w = Window.partitionBy("event_slug").orderBy(F.col("ingest_ts").desc())
latest = (bronze
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn"))

if LOOKBACK_DAYS is not None:
    # Anchor on the last ingest, not on today: rerunning tomorrow gives the same scope.
    last_ingest = bronze.agg(F.max(F.to_date("ingest_ts"))).first()[0]
    cutoff = last_ingest - datetime.timedelta(days=LOOKBACK_DAYS)
    # Events silver doesn't have yet are always in scope (catch-up after a missed run).
    in_silver = (spark.read.table("silver_layer.dbo.silv_event")
                 .filter("source = 'cito'").select("event_id"))
    latest = (latest
        .withColumn("event_id", F.get_json_object("payload", "$.data.event.dataId"))
        .join(in_silver.withColumn("in_silver", F.lit(True)), "event_id", "left")
        # SQL: WHERE event_date >= cutoff OR in_silver IS NULL
        .filter((F.to_date(F.substring("event_date", 1, 10)) >= F.lit(cutoff))
                | F.col("in_silver").isNull())
        .drop("event_id", "in_silver"))
    print(f"incremental: events since {cutoff} (last ingest {last_ingest} - {LOOKBACK_DAYS} days) + events not yet in silver")
else:
    print("full refresh: every event in bronze")
print("events in scope:", latest.count())

# Infer the JSON structure from ALL successful payloads, not just the ones in scope:
# a field that happens to be absent from 2 events would otherwise vanish from the
# schema and break the cells that read it.
payload_schema = spark.read.json(bronze.rdd.map(lambda r: r.payload)).schema

# Parse the raw JSON string into a structured Spark column.
parsed = latest.withColumn("p", F.from_json("payload", payload_schema))

# Inspect the metadata and event details for up to five events.
display(parsed.select(
    "event_slug",
    "ingest_ts",
    "p.data.event.dataId",
    "p.data.event.eventDate",
    "p.meta.roundStatRows"
).limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the bronze table and infer the structure of its raw JSON payloads.
bronze = spark.read.table("bronze_cito_event_stats")

payload_schema = spark.read.json(
    bronze.select("payload").rdd.map(lambda row: row.payload)
).schema

# Print the nested JSON fields and their data types.
payload_schema

# Inspect the parsed JSON structure.
parsed.select("p.*").printSchema()

# Select event metadata from the parsed JSON column.
display(parsed.select(
    "event_slug",
    "ingest_ts",
    F.col("p.data.event.dataId").alias("event_data_id"),
    F.col("p.data.event.eventDate").alias("event_date"),
    F.col("p.meta.roundStatRows").alias("round_stat_rows")
).limit(5))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# What Cito's text values look like. The mappings below were written without seeing them;
# anything they don't cover shows up as UNMAPPED and stops the write cell.
from pyspark.sql import functions as F
from pyspark.sql.window import Window

bouts = (parsed
    .select("event_slug", F.col("p.data.event").alias("e"), F.explode_outer("p.data.bouts").alias("b"))
    .filter(F.col("b").isNotNull()))

bouts.groupBy("b.status", "b.isCancelled", "b.method", "b.methodDetails").count().show(60, truncate=False)
bouts.groupBy("b.weightClass").count().show(30, truncate=False)
bouts.select(F.explode("b.fighters").alias("f")).groupBy("f.corner", "f.outcome").count().show()
bouts.select(F.explode("b.roundStats").alias("r")).select("r.significantStrikes", "r.takedowns", "r.head", "r.controlTime").show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================
# CELL 4 — Helper functions + two sanity checks
# ---------------------------------------------------------------------
# What:   (1) defines four small tools the later cells reuse,
#         (2) confirms the round data nested inside each bout is complete,
#         (3) removes cancelled bouts (no result, so they are not fights).
# Reads:  `parsed` (cell 2), `bouts` (cell 3)
# Makes:  problems, mmss(), x_of_y(), norm_name(); `bouts` without cancelled bouts
# Expect: the three round counts equal (252); bouts 62, cancelled 0
# =====================================================================

import re, unicodedata
from pyspark.sql.types import StringType

# --- Fault list ------------------------------------------------------
# Every check in cells 4–8 writes one line here: problems["<what>"] = <number of bad rows>.
# Cell 9 only writes to silver if every number is 0.
# Think of it as a sign-off tab in Excel where every row must say 0.
problems = {}

# --- mmss: "m:ss" text -> seconds -------------------------------------
# Used for controlTime ("1:28" -> 88) and resultTime (when in the round the fight ended).
def mmss(c):
    # regexp_extract(text, pattern, N) returns the piece of text caught by bracket group N.
    # Pattern ^(\d+):(\d{2})$ = start, digits (group 1), colon, exactly 2 digits (group 2), end.
    m = F.regexp_extract(c, r"^(\d+):(\d{2})$", 1)   # minutes, e.g. "1"
    s = F.regexp_extract(c, r"^(\d+):(\d{2})$", 2)   # seconds, e.g. "28"
    # If the text doesn't fit the pattern, regexp_extract returns "" (empty text).
    # F.when(...) without .otherwise() returns null when the condition is false,
    # so bad text becomes null instead of a wrong number.
    # SQL: CASE WHEN m <> '' THEN m * 60 + s END
    return F.when(m != "", m.cast("int") * 60 + s.cast("int"))

# --- x_of_y: "16 of 40" -> 16 or 40 -----------------------------------
# Cito stores most strike stats as text "<landed> of <attempted>".
# i=1 returns landed (16), i=2 returns attempted (40).
XY = r"^\s*(\d+)\s*of\s*(\d+)\s*$"   # \s* = optional spaces around the numbers
def x_of_y(c, i):
    v = F.regexp_extract(c, XY, i)
    return F.when(v != "", v.cast("int"))   # same rule as mmss: bad text -> null

# --- norm_name: make two spellings of a name comparable ----------------
# "José Aldo", "jose aldo", "Jose  Aldo" -> "jose aldo".
# This is a UDF (user-defined function): plain Python that Spark runs once per row.
# Slower than built-in F. functions, but fine for ~120 names, and Python can strip
# accents, which no built-in Spark function does.
@F.udf(StringType())          # tells Spark the function returns text
def norm_name(s):
    if s is None:             # Spark's null arrives in Python as None: null in -> null out
        return None
    # NFKD splits "é" into "e" + a separate accent mark; the ASCII round-trip drops the mark
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    # Lowercase; anything that isn't a-z or space becomes a space ("St-Pierre" -> "st pierre");
    # split + join collapses double spaces and trims both ends
    return " ".join(re.sub(r"[^a-z ]", " ", s.lower()).split())

# --- Check 1: is the nested round copy complete? -----------------------
# Cito sends round stats twice: inside each bout (b.roundStats) and once for the whole
# card (data.roundStats). We use the nested copy, because each round then already sits
# next to its bout. Before trusting it, compare it with the other copy and with Cito's
# own total in meta.roundStatRows.
nested_n = bouts.select(F.explode("b.roundStats")).count()          # rows via each bout
top_n    = parsed.select(F.explode("p.data.roundStats")).count()    # rows via the top-level copy
meta_n   = parsed.agg(F.sum("p.meta.roundStatRows")).first()[0]     # .first()[0] pulls the single number out
print("round-stat rows  nested:", nested_n, " top-level:", top_n, " meta:", meta_n, "(expect all equal, 252)")
problems["nested round copy incomplete"] = 0 if nested_n == meta_n else 1

# --- Check 2: drop cancelled bouts --------------------------------------
# A cancelled bout has no winner, method or rounds, so it can't be a row in silv_fight.
# coalesce(isCancelled, False): a missing value counts as "not cancelled".
# SQL: COALESCE(isCancelled, 0)
print("bouts:", bouts.count(), " cancelled:", bouts.filter(F.coalesce("b.isCancelled", F.lit(False))).count())
# ~ means NOT: keep the bouts that are not cancelled.
# SQL: WHERE NOT COALESCE(isCancelled, 0)
bouts = bouts.filter(~F.coalesce(F.col("b.isCancelled"), F.lit(False)))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================
# CELL 5 — Who is who: match Cito fighters to fighters already in silver
# ---------------------------------------------------------------------
# What:   gives every Cito fighter a silver fighter_id, and decides which
#         fighter in each bout is f1 and which is f2.
# Why:    Elo follows a fighter across fights. A known fighter who got a new ID
#         would restart at the default rating. Cito has no UFCStats fighter IDs,
#         so the match is made on the name (decisions.md, 2026-10-21).
# Rule:   in MANUAL_MAP                       -> that ID, always wins     (manual)
#         name matches ONE Kaggle fighter     -> that fighter's UFCStats ID (exact_name)
#         name matches SEVERAL Kaggle fighters -> stop, you decide by hand (ambiguous)
#         name matches nobody                 -> new fighter, "cito_<uuid>" (unmatched)
# Reads:  `bouts` (cell 4), silver_layer.dbo.silv_fighter
# Makes:  silv_fighter_xref, silv_fighter_cito (new fighters only), sides, f1, f2
# Expect: most fighters exact_name; debuts in the unmatched list; ambiguous = 0
# =====================================================================

# --- One row per fighter per bout --------------------------------------
# b.fighters is an array of 2 (red + blue): explode -> 2 rows per bout, 62 x 2 = 124
bout_fighters = (bouts
    .select(F.col("b.id").alias("bout_id"), F.explode("b.fighters").alias("f"))
    # pull the fields we need out of the fighter struct, under our own names
    .select("bout_id",
            F.col("f.fighterId").alias("cito_fighter_id"),    # Cito's UUID for the fighter
            F.col("f.fighterSlug").alias("fighter_slug"),     # also how round stats name the fighter
            F.col("f.fighterName").alias("fighter_name"),
            F.col("f.profile.nickname").alias("nickname")))   # struct inside a struct: f -> profile -> nickname

# One row per fighter: someone who fought twice in the window appears once.
# dropDuplicates(["x"]) = keep one row per value of x.
cito_fighters = bout_fighters.drop("bout_id").dropDuplicates(["cito_fighter_id"])

# --- The fighters silver already knows, by normalised name -------------
known = (spark.read.table("silver_layer.dbo.silv_fighter")
    # other sources only: Cito's own rows from an earlier run must not match themselves
    .filter("source != 'cito' AND full_name IS NOT NULL")
    .withColumn("name_key", norm_name("full_name"))
    # SQL: SELECT name_key, COUNT(*) AS n, FIRST(fighter_id) ... GROUP BY name_key
    # n tells us whether a name belongs to one fighter or to several
    .groupBy("name_key")
    .agg(F.count("*").alias("n"), F.first("fighter_id").alias("known_fighter_id")))

# --- Hand-made matches --------------------------------------------------
# For spelling variants and shared names. Key = Cito UUID (from the review list below),
# value = UFCStats fighter ID (the 16-character code at the end of the fighter's ufcstats.com URL).
MANUAL_MAP = {
    # "uuid-from-cito": "a1b2c3d4e5f6a7b8",   # <fighter name>, checked on ufcstats.com
}
# Turn the Python dict into a 2-column table so it can be joined.
# The schema text is needed so this also works while MANUAL_MAP is empty.
manual = spark.createDataFrame(list(MANUAL_MAP.items()), "cito_fighter_id string, manual_fighter_id string")

# --- The crosswalk: Cito fighter -> silver fighter_id --------------------
xref = (cito_fighters
    .withColumn("name_key", norm_name("fighter_name"))
    # LEFT JOINs: every Cito fighter stays; match info is filled in where it exists
    .join(known, "name_key", "left")
    .join(manual, "cito_fighter_id", "left")
    # How was the fighter matched? The order of the WHENs is the priority order.
    .withColumn("match_method",
        F.when(F.col("manual_fighter_id").isNotNull(), "manual")
         .when(F.col("n") == 1, "exact_name")
         .when(F.col("n") > 1, "ambiguous")
         .otherwise("unmatched"))                    # n is null = name not found
    # Which ID does the fighter get?
    .withColumn("fighter_id",
        F.when(F.col("match_method") == "manual", F.col("manual_fighter_id"))
         .when(F.col("match_method") == "exact_name", F.col("known_fighter_id"))
         .otherwise(F.concat(F.lit("cito_"), F.col("cito_fighter_id")))))   # new: "cito_" + UUID

# --- Review ------------------------------------------------------------------
xref.groupBy("match_method").count().show()
# Debut fighters belong in this list. A name you know from before 16 May is a
# spelling variant: look it up on ufcstats.com and add a MANUAL_MAP line.
display(xref.filter(F.col("match_method").isin("unmatched", "ambiguous"))
            .select("match_method", "fighter_name", "fighter_slug", "cito_fighter_id")
            .orderBy("match_method", "fighter_name"))
# Ambiguous stops the run: a guess could give one fighter another fighter's Elo history.
problems["ambiguous names (add to MANUAL_MAP)"] = xref.filter("match_method = 'ambiguous'").count()

# --- Tables for cell 9 ----------------------------------------------------------
# silv_fighter_xref: the crosswalk itself, column names from silver-contract.md
silv_fighter_xref = xref.select(F.lit("cito").alias("source"),
                                F.col("cito_fighter_id").alias("source_fighter_id"),
                                "fighter_id", "match_method")
# silv_fighter: NEW fighters only (cito_ IDs); matched fighters already have their Kaggle row.
# Cito's event data has no dob/height/reach: cell 8 fills those columns with null.
silv_fighter_cito = (xref.filter(F.col("fighter_id").startswith("cito_"))
    .select("fighter_id", F.col("fighter_name").alias("full_name"), "nickname", F.lit("cito").alias("source")))

# --- f1 / f2 ------------------------------------------------------------------------
# Not red/blue: red wins more often (cell 3: 34 vs 27 bouts), so "f1 = red" would let
# the model learn the corner instead of the fighter. Alphabetical by slug is fixed
# and has nothing to do with who wins.
# Same window pattern as the snapshot filter in cell 1: number the rows inside each bout.
w_side = Window.partitionBy("bout_id").orderBy("fighter_slug")
sides = (bout_fighters
    .join(xref.select("cito_fighter_id", "fighter_id"), "cito_fighter_id", "left")   # attach the silver ID
    .withColumn("side", F.row_number().over(w_side)))                                   # 1 or 2
# Every bout must have exactly 2 fighters.
# SQL: SELECT bout_id FROM sides GROUP BY bout_id HAVING COUNT(*) <> 2
problems["bouts without exactly 2 fighters"] = sides.groupBy("bout_id").count().filter("count != 2").count()

# One small table per side, to join onto the fight in cell 6.
# The slug comes along because the winner and the round stats are identified by slug.
f1 = sides.filter("side = 1").select("bout_id", F.col("fighter_id").alias("f1_fighter_id"), F.col("fighter_slug").alias("f1_slug"))
f2 = sides.filter("side = 2").select("bout_id", F.col("fighter_id").alias("f2_fighter_id"), F.col("fighter_slug").alias("f2_slug"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Names Cito gets wrong ------------------------------------------------
# Keyed on Cito UUID: (real name, nickname). Applied before matching, so the real
# name is what gets matched now and stored in silv_fighter.
NAME_FIX = {
    "c95e368b-b71f-4e97-a268-377b41c75e58": ("Valesca Machado", "Tina Black"),   # name field held her nickname (21 Oct)
}
name_fix = spark.createDataFrame([(k, v[0], v[1]) for k, v in NAME_FIX.items()],
                                 "cito_fighter_id string, fixed_name string, fixed_nickname string")
# LEFT JOIN + coalesce = use the fix where one exists, otherwise keep Cito's value
cito_fighters = (cito_fighters
    .join(name_fix, "cito_fighter_id", "left")
    .withColumn("fighter_name", F.coalesce("fixed_name", "fighter_name"))
    .withColumn("nickname", F.coalesce("fixed_nickname", "nickname"))
    .drop("fixed_name", "fixed_nickname"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================
# CELL 6 — silv_event and silv_fight: one row per event, one row per fight
# ---------------------------------------------------------------------
# What:   picks the event and bout fields silver needs, renames them to silver
#         names, and translates Cito's text into silver's fixed values
#         (method, decision_type, outcome, weight_class, gender).
# Why:    silver is source-agnostic: a Cito fight must look exactly like a Kaggle
#         fight. This cell is the only place Cito's field names and spellings live.
# Reads:  `parsed` (cell 2), `bouts` (cell 4), f1/f2 (cell 5)
# Makes:  silv_event_cito; `fight` (still has working columns, dropped in cell 8)
# Expect: 5 events, 62 fights; method/outcome UNMAPPED = 0; 1 draw
# Null on purpose: scheduled_rounds (no Cito field); rankings (rankText is the rank
#         when Cito fetched it, not at the fight -> leakage); odds (free tier 403).
# =====================================================================

# --- silv_event ---------------------------------------------------------
# parsed already has one row per event (cell 1's snapshot filter), so the grain is right.
silv_event_cito = parsed.select(
    F.col("p.data.event.dataId").alias("event_id"),            # = UFCStats event ID
    F.col("p.data.event.title").alias("event_name"),
    F.to_date("p.data.event.eventDate").alias("event_date"),   # text "2026-09-19" -> date type (contract rule)
    F.col("p.data.event.city").alias("event_city"),
    F.col("p.data.event.country").alias("event_country"),
    F.lit("MMA").alias("sport"),                                # F.lit = the same constant on every row
    F.lit("UFC").alias("promotion"),
    F.lit("cito").alias("source"))
# event_state isn't in Cito; cell 8 adds it as null.

# --- Expressions used several times below -------------------------------
mth = F.lower(F.col("b.method"))   # "U-DEC" -> "u-dec": compare in lowercase only
# method + methodDetails as one text, e.g. "ko/tko punches".
# concat_ws(" ", a, b) joins with a space and skips nulls. Excel: TEXTJOIN(" ", TRUE, a, b)
txt = F.lower(F.concat_ws(" ", F.col("b.method"), F.col("b.methodDetails")))
# Weight class: strip "UFC ", "Interim ", " Title", " Bout".
# "Women's Flyweight Title" -> "Women's Flyweight". The title flag lives in titleBout.
wc  = F.trim(F.regexp_replace(F.col("b.weightClass"), r"^UFC |Interim | Title| Bout$", ""))

# --- Pick and rename the bout fields -------------------------------------
fight = (bouts
    .select(
        F.col("b.id").alias("bout_id"),                              # working column: joins f1/f2 and rounds
        F.concat(F.lit("cito_"), F.col("b.id")).alias("fight_id"),   # silver key, e.g. "cito_13002"
        F.col("e.dataId").alias("event_id"),
        mth.alias("mth"),                                            # working column
        txt.alias("txt"),                                            # working column
        F.col("b.winnerFighterSlug").alias("winner_slug"),           # working column: the winner, as a slug
        # Draws are marked on the fighters (outcome = "draw"), not in the method text (cell 3).
        # F.exists(array, test) = True if ANY element passes the test, without exploding.
        # SQL: EXISTS (SELECT 1 FROM fighters WHERE outcome = 'draw')
        F.exists("b.fighters", lambda f: f["outcome"] == "draw").alias("is_draw"),   # working column
        F.when(wc == "Catch Weight", "Catchweight").otherwise(wc).alias("weight_class"),  # Kaggle's spelling
        F.coalesce(F.col("b.titleBout"), F.lit(False)).alias("title_fight"),  # missing -> not a title fight
        F.col("b.referee.name").alias("referee"),
        F.trim(F.col("b.methodDetails")).alias("method_detail"),     # e.g. "Rear Naked Choke"
        F.col("b.resultRound").cast("int").alias("finish_round"),    # last round fought
        mmss(F.col("b.resultTime")).alias("finish_time_sec"))        # "2:18" -> 138 (helper, cell 4)
    # Attach f1/f2 from cell 5. LEFT JOIN: a bout missing a fighter stays and is caught by the null checks.
    .join(f1, "bout_id", "left")
    .join(f2, "bout_id", "left")
    # event_date copied onto the fight (contract: convenience copy)
    .join(silv_event_cito.select("event_id", "event_date"), "event_id", "left"))

# --- Translate Cito's values into silver's fixed lists ---------------------
m = F.col("mth"); t = F.col("txt")   # short names to keep the WHEN chains readable
fight = (fight
    # method: 5 allowed values. Cito writes "Decision - Unanimous" AND "U-DEC",
    # "Submission" AND "SUB" (cell 3), so match on a fragment, not the full text.
    # Anything else -> "UNMAPPED", which stops the run instead of slipping through.
    .withColumn("method",
        F.when(m.contains("dec"), "Decision")
         .when(m.contains("ko") | m.contains("doctor"), "KO/TKO")
         .when(m.contains("sub"), "Submission")
         .when(m.contains("dq") | m.contains("disqual"), "DQ")
         .when(m.contains("could not continue"), "Could Not Continue")
         .otherwise("UNMAPPED"))
    # decision_type: decisions only, null for everything else.
    # rlike = regex test. r"\bu-?dec" = "u-dec" or "udec" at the start of a word.
    .withColumn("decision_type",
        F.when(F.col("method") != "Decision", None)
         .when(t.contains("unanimous") | t.rlike(r"\bu-?dec"), "Unanimous")
         .when(t.contains("split")     | t.rlike(r"\bs-?dec"), "Split")
         .when(t.contains("majority")  | t.rlike(r"\bm-?dec"), "Majority"))
    # outcome: win if Cito names a winner; draw if a fighter is marked draw.
    # No contest hasn't appeared yet: the text test is a guess, and UNMAPPED catches it if wrong.
    .withColumn("outcome",
        F.when(F.col("winner_slug").isNotNull(), "win")
         .when(F.col("is_draw"), "draw")
         .when(t.contains("no contest") | t.contains("overturn"), "no_contest")
         .otherwise("UNMAPPED"))
    # winner_fighter_id: compare the winner's slug with each side's slug, take that side's ID.
    # Null when nobody won (draw / no contest), as the contract says.
    .withColumn("winner_fighter_id",
        F.when(F.col("winner_slug") == F.col("f1_slug"), F.col("f1_fighter_id"))
         .when(F.col("winner_slug") == F.col("f2_slug"), F.col("f2_fighter_id")))
    # gender: Cito has no field; every women's division starts with "Women's"
    .withColumn("gender", F.when(F.col("weight_class").startswith("Women's"), "F").otherwise("M"))
    .withColumn("source", F.lit("cito")))

# --- Checks -------------------------------------------------------------------
# Weight classes silver already uses (from Kaggle). A new value would add a member to
# Power BI's weight-class slicer -> stop and map it instead.
# .collect() brings the values back to Python as rows; row[0] takes the value out.
known_wc = [row[0] for row in spark.read.table("silver_layer.dbo.silv_fight")
                                  .filter("source != 'cito'").select("weight_class").distinct().collect()]

fight.groupBy("method", "decision_type", "outcome").count().show(truncate=False)   # eyeball the translation
problems["method UNMAPPED"]  = fight.filter("method = 'UNMAPPED'").count()
problems["outcome UNMAPPED"] = fight.filter("outcome = 'UNMAPPED'").count()
# A win must point at one of the two fighters; if not, the slugs didn't line up
problems["win without winner_fighter_id"] = fight.filter("outcome = 'win' AND winner_fighter_id IS NULL").count()
problems["weight_class not in the Kaggle list"] = fight.filter(~F.col("weight_class").isin(known_wc)).count()
# Allowed to be null, but worth seeing: a decision whose type couldn't be read
print("decisions without decision_type (allowed, check):", fight.filter("method = 'Decision' AND decision_type IS NULL").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================
# CELL 7 — silv_round: one row per fight x round, both fighters side by side
# ---------------------------------------------------------------------
# What:   Cito gives one row per FIGHTER per round (2 rows per round), stats as text.
#         (1) text -> numbers, (2) which row is f1 / f2 (by slug, else by elimination),
#         (3) fold the 2 rows into 1 with f1_/f2_ columns, (4) a row for every round
#         fought, even without stats (stats_missing).
# Why:    silv_round grain = fight x round with f1_/f2_ pairs, same shape as Kaggle.
# Reads:  `bouts` (cell 4), `sides` (cell 5), `fight` (cell 6)
# Makes:  silv_round_cito
# Expect: 9 rows matched by elimination; 126 rounds, all with stats; checks 0
# =====================================================================

# Stored as "<landed> of <attempted>": each becomes _succ and _att
X_OF_Y = {"significantStrikes": "sig_strikes", "totalStrikes": "total_strikes", "takedowns": "td",
          "head": "head", "body": "body", "leg": "leg",
          "distance": "distance", "clinch": "clinch", "ground": "ground"}
COUNTS = {"knockdowns": "knockdowns", "submissionAttempts": "submission_att", "reversals": "reversals"}
# The 22 stats, in the contract's order
STATS = ["knockdowns", "sig_strikes_att", "sig_strikes_succ", "total_strikes_att", "total_strikes_succ",
         "td_att", "td_succ", "submission_att", "reversals", "ctrl_sec",
         "head_att", "head_succ", "body_att", "body_succ", "leg_att", "leg_succ",
         "distance_att", "distance_succ", "clinch_att", "clinch_succ", "ground_att", "ground_succ"]

# One row per fighter per round, from the copy nested in each bout -> 252 rows
r = bouts.select(F.col("b.id").alias("bout_id"), F.explode("b.roundStats").alias("r"))

# --- Text -> numbers ----------------------------------------------------------
cols = ["bout_id",
        F.col("r.fighterSlug").alias("fighter_slug"),
        F.col("r.round").cast("int").alias("round"),
        mmss(F.col("r.controlTime")).alias("ctrl_sec")]
for src, base in X_OF_Y.items():
    # "16 of 40" -> sig_strikes_succ = 16, sig_strikes_att = 40
    cols += [x_of_y(F.col(f"r.{src}"), 1).alias(f"{base}_succ"),
             x_of_y(F.col(f"r.{src}"), 2).alias(f"{base}_att")]
cols += [F.col(f"r.{src}").cast("int").alias(base) for src, base in COUNTS.items()]
fr = r.select(cols)   # fr = "fighter rounds"

# Non-null text that gives a null number = a format the helpers don't expect.
# One big OR condition over all text fields, then count the rows where it's true.
bad = F.col("r.controlTime").isNotNull() & mmss(F.col("r.controlTime")).isNull()
for src in X_OF_Y:
    bad = bad | (F.col(f"r.{src}").isNotNull() & x_of_y(F.col(f"r.{src}"), 1).isNull())
problems["stat text that failed to parse"] = r.filter(bad).count()

# --- Which side is each row? ----------------------------------------------------------
# First by slug. Round stats sometimes spell the slug differently from the fighter list
# (timmy/timothy, pitbull/freire, reversed name order, nickname): 9 rows on 21 Oct.
fr = fr.join(sides.select("bout_id", "fighter_slug", "side"), ["bout_id", "fighter_slug"], "left")

# Then by elimination: a round has exactly two rows. If one row found its side,
# the other row is the opposite side (3 - 1 = 2, 3 - 2 = 1).
# SQL: COALESCE(side, 3 - MAX(side) OVER (PARTITION BY bout_id, round))
w_round = Window.partitionBy("bout_id", "round")
print("rows matched by elimination:", fr.filter(F.col("side").isNull()).count(), "(9 on 21 Oct)")
fr = fr.withColumn("side", F.coalesce("side", 3 - F.max("side").over(w_round)))

# Still null = neither row in the round matched -> elimination can't help
problems["round rows not matched to a corner"] = fr.filter(F.col("side").isNull()).count()
# Two rows on the same side would be silently merged by the pivot below
problems["duplicate fighter x round rows"] = fr.groupBy("bout_id", "round", "side").count().filter("count > 1").count()

# --- Pivot: 2 rows per round -> 1 row with f1_/f2_ columns ---------------------------
# Conditional aggregation (the SUMIF pattern), for each stat s and side n:
#   SQL: MAX(CASE WHEN side = n THEN s END) AS fn_s ... GROUP BY bout_id, round
# MAX just picks the one non-null value per group.
pivot = fr.groupBy("bout_id", "round").agg(
    *[F.max(F.when(F.col("side") == n, F.col(s))).alias(f"f{n}_{s}") for n in (1, 2) for s in STATS])

# --- Every round fought gets a row -------------------------------------------------
# sequence(1, finish_round) -> [1, 2, 3]; explode -> one row per round (same rule as Kaggle)
base = fight.select("bout_id", "fight_id",
                    F.explode(F.sequence(F.lit(1), F.col("finish_round"))).alias("round"))
# Stats for a round after the fight ended would be lost in the LEFT JOIN below.
# left_anti = rows in pivot with NO match in base. SQL: WHERE NOT EXISTS (...)
problems["stats beyond finish_round"] = pivot.join(base, ["bout_id", "round"], "left_anti").count()

STAT_COLS = [f"f{n}_{s}" for n in (1, 2) for s in STATS]
silv_round_cito = (base
    .join(pivot, ["bout_id", "round"], "left")
    # stats_missing = every stat column null (coalesce finds no non-null value)
    .withColumn("stats_missing", F.coalesce(*[F.col(c) for c in STAT_COLS]).isNull())
    .withColumn("source", F.lit("cito"))
    .drop("bout_id"))

# --- Landed can't exceed attempted ----------------------------------------------------
succ_gt_att = F.lit(False)
for n in (1, 2):
    for s in ["sig_strikes", "total_strikes", "td", "head", "body", "leg", "distance", "clinch", "ground"]:
        succ_gt_att = succ_gt_att | (F.col(f"f{n}_{s}_succ") > F.col(f"f{n}_{s}_att"))
problems["succ > att"] = silv_round_cito.filter(succ_gt_att).count()

print("silv_round rows:", silv_round_cito.count(),
      " with stats:", silv_round_cito.filter("NOT stats_missing").count(), "(expect 126 = 252 / 2)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================
# CELL 8 — Match the silver table shapes, then run every check
# ---------------------------------------------------------------------
# What:   (1) lines each Cito table up with the existing silver table: same columns,
#         same order, same types; contract columns Cito lacks become typed nulls.
#         (2) runs the key, reference and not-null checks, (3) prints the fault list.
# Why:    the MERGE (cell 9) writes into tables Kaggle already fills, so the
#         shape must match exactly. And nothing gets written before the checks pass.
# Reads:  the cito tables from cells 5–7, the silver tables
# Makes:  final silv_*_cito tables; a printed OK/FAIL list
# Expect: every line OK; events 5, fights 62
# =====================================================================

def to_target(df, table):
    tgt = spark.read.table(table)                     # the existing silver table, used as the template
    # A column we made that the table doesn't have = a typo or a working column we forgot to drop
    extra = set(df.columns) - set(tgt.columns)
    assert not extra, f"{table}: columns not in the table: {extra}"
    # Columns the table has that we don't (e.g. odds) -> printed, so you see what becomes null
    print(table, "- filled with null:", [c for c in tgt.columns if c not in df.columns])
    # tgt.dtypes = list of (column, type). For each: our column cast to the table's type,
    # or a null of that type. SQL: SELECT CAST(x AS int) AS x, CAST(NULL AS double) AS f1_odds_decimal, ...
    return df.select([(F.col(c) if c in df.columns else F.lit(None)).cast(t).alias(c) for c, t in tgt.dtypes])

S = "silver_layer.dbo."
silv_event_cito   = to_target(silv_event_cito, S + "silv_event")
# drop the working columns first, or to_target stops on them
silv_fight_cito   = to_target(fight.drop("bout_id", "mth", "txt", "winner_slug", "is_draw", "f1_slug", "f2_slug"),
                              S + "silv_fight")
silv_round_cito   = to_target(silv_round_cito, S + "silv_round")
silv_fighter_cito = to_target(silv_fighter_cito, S + "silv_fighter")

# --- Keys: each natural key appears once -------------------------------------
# rows minus distinct keys = number of duplicates
problems["duplicate event_id"] = silv_event_cito.count() - silv_event_cito.select("event_id").distinct().count()
problems["duplicate fight_id"] = silv_fight_cito.count() - silv_fight_cito.select("fight_id").distinct().count()
problems["duplicate fight_id + round"] = silv_round_cito.count() - silv_round_cito.select("fight_id", "round").distinct().count()
# The same event from two sources would be two rows with one key.
# left_semi = keep rows that HAVE a match. SQL: WHERE EXISTS (...)
# Overlap with other sources is kept on purpose (decisions.md 2026-10-22): gold keeps one per fight.
# So this is reported, not a problem that blocks the write.
overlap_events = (silv_event_cito
    .join(spark.read.table(S + "silv_event").filter("source != 'cito'"), "event_id", "left_semi").count())
print("events also in silver from another source (overlap, expected 4-5):", overlap_events)

# --- References: every ID points at something that exists ---------------------
# Fighters silver will contain after the write: the other sources' + Cito's new ones
all_fighters = (spark.read.table(S + "silv_fighter").filter("source != 'cito'").select("fighter_id")
                .union(silv_fighter_cito.select("fighter_id")))
# Put f1 and f2 into one column (array + explode), then find IDs with no fighter row
problems["fight fighter_id not in silv_fighter"] = (silv_fight_cito
    .select(F.explode(F.array("f1_fighter_id", "f2_fighter_id")).alias("fighter_id"))
    .join(all_fighters, "fighter_id", "left_anti").count())
problems["round fight_id not in silv_fight"] = silv_round_cito.join(silv_fight_cito, "fight_id", "left_anti").count()

# --- Not-null columns, per the contract (the same lists as in 20_clean_kaggle) ----------
NOT_NULL = {
    "silv_event":   (silv_event_cito,   ["event_id", "event_name", "event_date", "sport", "promotion", "source"]),
    "silv_fight":   (silv_fight_cito,   ["fight_id", "event_id", "event_date", "f1_fighter_id", "f2_fighter_id", "outcome",
                                         "weight_class", "gender", "title_fight", "method", "finish_round", "finish_time_sec", "source"]),
    "silv_round":   (silv_round_cito,   ["fight_id", "round", "stats_missing", "source"]),
    "silv_fighter": (silv_fighter_cito, ["fighter_id", "full_name", "source"]),
}
for t, (d, cs) in NOT_NULL.items():
    for c in cs:
        problems[f"NULL in {t}.{c}"] = d.filter(F.col(c).isNull()).count()

# --- The fault list --------------------------------------------------------------
for k, v in problems.items():
    print(("OK  " if v == 0 else "FAIL"), k, v)
print("events:", silv_event_cito.count(), " fights:", silv_fight_cito.count(),
      " rounds:", silv_round_cito.count(), " new fighters:", silv_fighter_cito.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================
# CELL 9 — MERGE Cito's rows into silver, then read them back
# ---------------------------------------------------------------------
# What:   one MERGE per silver table, on its natural key, for the events in scope (cell 1):
#           new key                           -> INSERT
#           same key, some value changed      -> UPDATE (Cito corrected / late stats)
#           same key, every value identical   -> nothing (no rewrite)
#           in silver, gone from the snapshot -> DELETE, only inside the events in scope
#         Rows of other sources are never touched: every ON and DELETE also says source = 'cito'.
# Why:    Cito re-sends the same events on every load. MERGE writes only what changed,
#         so a rerun right after prints "no change" on every table. That is the idempotency test.
# Expect: after new data: a few inserts/updates; immediate rerun: "no change" x 5
# =====================================================================

from delta.tables import DeltaTable

# Stops Run all here if any check in cells 4–8 failed: nothing gets written
assert all(v == 0 for v in problems.values()), "Checks failed - see the FAIL lines in cell 8; nothing written"

# --- Delete scope ----------------------------------------------------------------
# Only rows belonging to events we just re-read may be deleted. An event outside the
# scope was not looked at, so its absence from the source means nothing.
scope_events = [r.event_id for r in silv_event_cito.select("event_id").collect()]
# Fights of those events: the ones silver has now + the ones in the new snapshot.
# Read BEFORE the fight MERGE, or a deleted fight's rounds would be out of scope.
scope_fights = [r.fight_id for r in (spark.read.table(S + "silv_fight")
    .filter(F.col("source") == "cito").filter(F.col("event_id").isin(scope_events))
    .select("fight_id").union(silv_fight_cito.select("fight_id")).distinct().collect())]

def in_list(col, values):
    # SQL text for "col IN ('a', 'b')"; FALSE when the list is empty (deletes nothing)
    return f"t.{col} IN ({', '.join(repr(v) for v in values)})" if values else "FALSE"

def merge_cito(df, table, keys, delete_scope=None):
    if not spark.catalog.tableExists(table):          # first run of a new table: plain create
        df.write.format("delta").saveAsTable(table)
        print(f"{table:40s} created"); return
    tgt = DeltaTable.forName(spark, table)
    v_before = tgt.history(1).first()["version"]      # latest version before this MERGE
    # ON: natural key + source. Kagglesk has the same event_id for overlap events;
    # without the source condition the MERGE would update Kaggle's row.
    on = " AND ".join(f"t.`{k}` = s.`{k}`" for k in keys) + " AND t.source = 'cito'"
    # "Something changed" = any non-key column differs. <=> is null-safe equality:
    # NULL <=> NULL is true, so a null that stays null doesn't count as a change.
    # SQL Server: NOT (t.x = s.x OR (t.x IS NULL AND s.x IS NULL))
    changed = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in df.columns if c not in keys)
    m = (tgt.alias("t")
         .merge(df.alias("s"), on)
         .whenMatchedUpdateAll(condition=changed)     # matched + identical -> no update at all
         .whenNotMatchedInsertAll())
    if delete_scope:
        # SQL Server: WHEN NOT MATCHED BY SOURCE AND ... THEN DELETE
        m = m.whenNotMatchedBySourceDelete(condition=f"t.source = 'cito' AND {delete_scope}")
    m.execute()
    # Nothing to change -> Delta writes no new version, and history(1) would show the
    # PREVIOUS write's metrics (that fooled us on 25 Oct). So compare versions first.
    h = tgt.history(1).first()
    if h["version"] == v_before:
        print(f"{table:40s} no change"); return
    om = h["operationMetrics"]
    print(f"{table:40s} v{h['version']}  inserted {om.get('numTargetRowsInserted')}"
          f"  updated {om.get('numTargetRowsUpdated')}  deleted {om.get('numTargetRowsDeleted')}")

# Order: fighters before fights, fights before rounds (parents before children)
merge_cito(silv_fighter_xref, S + "silv_fighter_xref", ["source_fighter_id"])
merge_cito(silv_fighter_cito, S + "silv_fighter",      ["fighter_id"])
merge_cito(silv_event_cito,   S + "silv_event",        ["event_id"])     # events are never deleted
merge_cito(silv_fight_cito,   S + "silv_fight",        ["fight_id"],          in_list("event_id", scope_events))
merge_cito(silv_round_cito,   S + "silv_round",        ["fight_id", "round"], in_list("fight_id", scope_fights))

# --- Fighters nobody points at any more ------------------------------------------
# A "cito_<uuid>" debut that a later run matched to a UFCStats ID (e.g. once kagglesk
# has the fighter): the xref now points elsewhere, so the cito_ row is an orphan.
# Deleted only if no fight still uses it.
xref_ids  = spark.read.table(S + "silv_fighter_xref").filter("source = 'cito'").select("fighter_id")
fight_ids = (spark.read.table(S + "silv_fight").filter("source = 'cito'")
             .select(F.explode(F.array("f1_fighter_id", "f2_fighter_id")).alias("fighter_id")))
orphans = [r.fighter_id for r in (spark.read.table(S + "silv_fighter").filter("source = 'cito'")
    .join(xref_ids, "fighter_id", "left_anti").join(fight_ids, "fighter_id", "left_anti").collect())]
if orphans:
    DeltaTable.forName(spark, S + "silv_fighter").delete((F.col("source") == "cito") & F.col("fighter_id").isin(orphans))
print("orphan cito_ fighters deleted:", len(orphans))

# A fight OUTSIDE the scope still carrying a cito_ ID the xref no longer uses was not
# re-processed: its fighter would split into two identities. Only a full refresh fixes it.
stale = fight_ids.filter(F.col("fighter_id").startswith("cito_")).join(xref_ids, "fighter_id", "left_anti").count()
print("fights with a stale cito_ fighter_id:", stale, "" if stale == 0 else "-> FULL REFRESH NEEDED (cell 1: LOOKBACK_DAYS = None)")

# Read back from the written tables, rows per source (lesson from 19 Oct: verify what was written)
for t in ["silv_event", "silv_fight", "silv_round", "silv_fighter", "silv_fighter_xref"]:
    print(t); spark.read.table(S + t).groupBy("source").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# =====================================================================
# Diagnose — which round rows have no corner, and what slugs does the bout have?
# ---------------------------------------------------------------------
# Expect: 9 rows; compare round_slug with the two bout_slugs next to it
# =====================================================================
nomatch = fr.filter(F.col("side").isNull()).select("bout_id", "round", F.col("fighter_slug").alias("round_slug"))

# The bout's two slugs collected into one list per bout, for comparison.
# collect_list = gather the group's values into an array. SQL Server: STRING_AGG
bout_slugs = sides.groupBy("bout_id").agg(F.collect_list("fighter_slug").alias("bout_slugs"))

display(nomatch.join(bout_slugs, "bout_id", "left").orderBy("bout_id", "round"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# How does Cito name her, and does Kaggle know her under her real name?
display(xref.filter(F.col("fighter_name").contains("Black") | F.col("fighter_name").contains("Machado"))
            .select("fighter_name", "fighter_slug", "match_method", "fighter_id"))
display(spark.read.table("silver_layer.dbo.silv_fighter").filter(F.col("full_name").contains("Machado")))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# --- Which side is each row? ----------------------------------------------------------
# First by slug. Round stats sometimes spell the slug differently from the fighter list
# (timmy/timothy, pitbull/freire, reversed name order, nickname): 9 rows on 21 Oct.
fr = fr.drop("side").join(sides.select("bout_id", "fighter_slug", "side"), ["bout_id", "fighter_slug"], "left")

# Then by elimination: a round has exactly two rows. If one row found its side,
# the other row is the opposite side (3 - 1 = 2, 3 - 2 = 1).
# max(side) over the bout+round = the side the matched row found.
# SQL: COALESCE(side, 3 - MAX(side) OVER (PARTITION BY bout_id, round))
w_round = Window.partitionBy("bout_id", "round")
print("rows matched by elimination:", fr.filter(F.col("side").isNull()).count(), "(9 on 21 Oct)")
fr = fr.withColumn("side", F.coalesce("side", 3 - F.max("side").over(w_round)))

# Still null = neither row in the round matched -> elimination can't help
problems["round rows not matched to a corner"] = fr.filter(F.col("side").isNull()).count()
# Elimination can't create a duplicate on its own, but a round with 3 rows would show up here
problems["duplicate fighter x round rows"] = fr.groupBy("bout_id", "round", "side").count().filter("count > 1").count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read back from the written tables, rows per source (lesson from 19 Oct: verify what was written)
for t in ["silv_event", "silv_fight", "silv_round", "silv_fighter", "silv_fighter_xref"]:
    print(t); spark.read.table(S + t).groupBy("source").count().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Cito per event: fights, rounds, rounds without stats
display(spark.sql("""
    SELECT e.event_date, e.event_name,
           count(DISTINCT f.fight_id)   AS fights,
           count(r.round)               AS rounds,
           count_if(r.stats_missing)    AS rounds_no_stats
    FROM silver_layer.dbo.silv_event e
    JOIN silver_layer.dbo.silv_fight f ON f.event_id = e.event_id AND f.source = 'cito'
    LEFT JOIN silver_layer.dbo.silv_round r ON r.fight_id = f.fight_id AND r.source = 'cito'
    WHERE e.source = 'cito'
    GROUP BY 1, 2 ORDER BY 1
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Profile completeness by debut year - report only, not blocking (decisions.md 2026-10-11)
# Why this exists: the baseline broke on 11 Oct because recent fighters had empty profiles
# (dob, height, reach), which was a scrape-timing artefact, not a fighter trait. Silver checked
# null COUNTS, which looked fine; the null RATE over time is what moved. This shows that rate
# per debut year, so the same gap shows up here before it reaches a model.

from pyspark.sql import functions as F

pc_fights = spark.read.table("silver_layer.dbo.silv_fight")

# Debut year = year of each fighter's first fight, from either corner.
# explode(array(f1, f2)) = SQL "SELECT f1 ... UNION ALL SELECT f2 ...": one row per fighter per fight
pc_debut = (pc_fights
    .select("event_date", F.explode(F.array("f1_fighter_id", "f2_fighter_id")).alias("fighter_id"))
    .groupBy("fighter_id")
    .agg(F.year(F.min("event_date")).alias("debut_year")))

# left join: fighters with no fight in silver keep debut_year = null (shown as their own row)
pc_profiles = spark.read.table("silver_layer.dbo.silv_fighter").join(pc_debut, "fighter_id", "left")

def pct_filled(c):
    # avg of a 1/0 flag = share of rows where the column is filled, like AVG(CASE WHEN c IS NOT NULL THEN 1 ELSE 0 END)
    return F.round(100 * F.avg(F.col(c).isNotNull().cast("int")), 1)

pc_all_missing = F.col("dob").isNull() & F.col("height_cm").isNull() & F.col("reach_cm").isNull()

completeness = (pc_profiles.groupBy("debut_year")
    .agg(F.count("*").alias("fighters"),
         pct_filled("dob").alias("dob_pct"),
         pct_filled("height_cm").alias("height_pct"),
         pct_filled("reach_cm").alias("reach_pct"),
         F.round(100 * F.avg(pc_all_missing.cast("int")), 1).alias("all_missing_pct"))
    .orderBy("debut_year"))

display(completeness)

# Flag recent debut years where profiles are thin (threshold is a starting point, not a decision)
thin = completeness.filter((F.col("debut_year") >= 2020) & (F.least("dob_pct", "height_pct", "reach_pct") < 90))
print("recent debut years with dob/height/reach < 90 % filled:", thin.count())
thin.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
