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

# ## 11_explore_cito — Cito API exploration (free tier)
# 
# Explores the Cito API on one recent event before any ingest is built: response shape, IDs, calls per event, and which fields exist. Its findings shape `10_ingest_cito` and the mapping in `20_clean_cito`.
# 
# - **Reads:** Cito API (free tier, last 30 days); API key from Key Vault `combat-sports-vang-v1`, secret `cito-api-key`
# - **Writes:** nothing
# - **Run order:** standalone; not part of the pipeline run
# - **Key output:** JSON shape per endpoint (event → bouts → rounds); whether IDs match UFCStats/Kaggle; API calls per event (free tier: 500/month); whether Cito carries odds, judge scores, fighter profiles (dob, reach, height)

# CELL ********************

#We connect to the key, after importing it into the secrets officer/member vault in Fabric (https://combat-sports-vang-v1.vault.azure.net/) and setting permissions.
key = notebookutils.credentials.getSecret("https://combat-sports-vang-v1.vault.azure.net/", "cito-api-key")
print(len(key))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import json, re, requests

BASE = "https://api.citoapi.com/api/v1"
API_KEY = notebookutils.credentials.getSecret("https://combat-sports-vang-v1.vault.azure.net/", "cito-api-key")

session = requests.Session()
session.headers.update({"x-api-key": API_KEY, "Accept": "application/json"})  # key in a header, never in the URL

calls = []  # one entry per API call (free tier: 500/month)
raw = {}    # every response, keyed by path, for the later cells

def get(path, **params):
    """GET one endpoint, log status + any quota headers, keep the JSON. Returns None on an error."""
    r = session.get(BASE + path, params=params, timeout=30)
    calls.append(path)
    quota = {k: v for k, v in r.headers.items() if "limit" in k.lower() or "quota" in k.lower()}
    print(f"[{len(calls)}] {r.status_code} {r.url} {quota}")
    if not r.ok:
        print("   ", r.text[:300])
        return None
    raw[path] = r.json()
    return raw[path]

def rows_of(obj):
    """First list of records in a response, however deeply it is wrapped."""
    if isinstance(obj, list):
        return obj
    if isinstance(obj, dict):
        for v in obj.values():
            found = rows_of(v)
            if found:
                return found
    return None

def peek(obj, n=2000):
    """Top-level keys, row count, and the first record pretty-printed (cut at n characters)."""
    if isinstance(obj, dict):
        print("top-level keys:", list(obj.keys()))
    rows = rows_of(obj)
    if rows:
        first = rows[0]
        print(f"{len(rows)} rows; first row keys:", list(first.keys()) if isinstance(first, dict) else type(first))
        print(json.dumps(first, indent=2, default=str)[:n])
    else:
        print(json.dumps(obj, indent=2, default=str)[:n])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

events = get("/ufc/events/recent", hasStats="true", limit=10)
peek(events)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Field names from the September test (dataId, eventDate, hasStats); adjust if peek shows others.
for e in rows_of(events):
    print({k: e.get(k) for k in ("id", "dataId", "slug", "name", "eventDate", "hasStats")})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

EVENT = rows_of(events)[0]["slug"]  # change by hand to pick another event
print("event:", EVENT)

bouts = get(f"/ufc/events/{EVENT}/bouts"); peek(bouts, 3000)
event_stats = get(f"/ufc/events/{EVENT}/stats"); peek(event_stats, 1500)
event_odds = get(f"/ufc/events/{EVENT}/odds"); peek(event_odds, 1500)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

BOUT = rows_of(bouts)[0]["id"]  # check in cell 4's output that "id" is the bout ID field
print("bout:", BOUT)

bout = get(f"/ufc/bouts/{BOUT}"); peek(bout, 3000)
rounds = get(f"/ufc/bouts/{BOUT}/rounds"); peek(rounds, 3000)
odds = get(f"/ufc/bouts/{BOUT}/odds"); peek(odds, 3000)

FIGHTER = ""  # paste a fighter slug from the bout output above, then rerun this line
if FIGHTER:
    fighter = get(f"/ufc/fighters/{FIGHTER}"); peek(fighter, 2000)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def find_keys(obj, words, path=""):
    """Every key path whose name contains one of the words (lists: first element only)."""
    hits = []
    if isinstance(obj, dict):
        for k, v in obj.items():
            p = f"{path}.{k}"
            if any(w in k.lower() for w in words):
                hits.append((p, str(v)[:60]))
            hits += find_keys(v, words, p)
    elif isinstance(obj, list) and obj:
        hits += find_keys(obj[0], words, path + "[0]")
    return hits

checks = {
    "odds":    ["odds", "moneyline", "price", "bookmaker", "market"],
    "judges":  ["judge", "scorecard"],
    "profile": ["dob", "birth", "reach", "height", "stance"],
}
for name, words in checks.items():
    print(f"\n== {name}")
    for src, obj in raw.items():
        for p, v in find_keys(obj, words):
            print(f"{src}{p} = {v}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# UFCStats IDs are 16 hex characters. Collect every such value in the responses and check silver.
# Expect events/fights after 16 May to match 0 (not in Kaggle); fighters with earlier UFC fights should match.
hex16 = set(re.findall(r'"([0-9a-f]{16})"', json.dumps(raw)))
print(len(hex16), "16-hex values in Cito responses")
for table, col in [("silv_event", "event_id"), ("silv_fight", "fight_id"), ("silv_fighter", "fighter_id")]:
    ids = {r[0] for r in spark.table(f"silver_layer.dbo.{table}").select(col).collect()}
    print(f"{table}.{col}: {len(hex16 & ids)} match")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Calls this budget
print(len(calls), "calls this run:")
print(*calls, sep="\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

card = raw[f"/ufc/events/{EVENT}/stats"]["data"]["bouts"]
for b in card:
    print(b["id"], b.get("dataId"), b["method"], "| scorecards:", b.get("scorecards"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fighter = get("/ufc/fighters/mike-malott")
f = fighter["data"]
print(sorted(f.keys()))
print({k: f.get(k) for k in f if k.lower() in ("dob", "dateofbirth", "birthdate", "age", "height", "reach", "stance", "weight")})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
