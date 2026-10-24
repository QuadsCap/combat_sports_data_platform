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

# ## 10_ingest_cito — Cito API → bronze
# 
# Lands raw Cito API responses in bronze: the recent-events list and the full stats card per event. Raw and immutable: one row per API response, JSON kept as a string, no parsing. Each run appends a new snapshot; silver picks the latest per event.
# 
# - **Reads:** Cito API (free tier, last 30 days); key from Key Vault `combat-sports-vang-v1`, secret `cito-api-key`
# - **Writes:** `bronze_layer.dbo.bronze_cito_events`, `bronze_layer.dbo.bronze_cito_event_stats` (append) 
# - **Run order:** first in the Cito path; before `20_clean_cito`
# - **Cost:**  ~6 calls per run (1 list + 1 per event with stats); limit 500/month, 10/min

# CELL ********************

import json, re, requests, time, datetime

# Retrieve the API key securely from Azure Key Vault.
# The key is sent in the request header, never in the URL.
BASE = "https://api.citoapi.com/api/v1"
API_KEY = notebookutils.credentials.getSecret("https://combat-sports-vang-v1.vault.azure.net/", "cito-api-key")

# Create a reusable session so all requests use the same authentication
# and response format settings.
session = requests.Session()
session.headers.update({"x-api-key": API_KEY, "Accept": "application/json"})  # key in a header, never in the URL


#timeout: fail loudly after 30 s instead of hanging forever.
# Keep track of API calls so we can log and identify each request.
calls = []

def call(path, **params):
    # Send a GET request to the specified endpoint with optional query
    # parameters and a 30-second timeout.
    # HTTP errors (e.g. 403 or 404) are still returned as responses.
    r = session.get(
        BASE + path,
        params=params,
        timeout=30
    )

    calls.append(path)

    print(f"[{len(calls)}] {r.status_code} {r.url}")
    

    return r


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, TimestampType
)

# Create one timestamp for this ingestion run.
# All rows written during this run will share the same timestamp,
# making it possible to identify which batch they belong to.
ingest_ts = datetime.datetime.now(
    datetime.timezone.utc
).replace(tzinfo=None)

# Define the schema for the bronze table.
# Each row stores metadata about an API request and its raw response.
bronze_schema = StructType([
    StructField("ingest_ts",   TimestampType(), False),
    StructField("source",      StringType(),    False),
    StructField("endpoint",    StringType(),    False),
    StructField("request_url", StringType(),    False),
    StructField("http_status", IntegerType(),   False),
    StructField("payload",     StringType(),    True),  # Raw response body, stored without parsing
])

# the 10 most recent events that have stats (DWCS still slips through; filtered in cell 3)
# with statistics included.
path = "/ufc/events/recent"
r = call(path, hasStats="true", limit=10)

# Create one row containing the ingestion timestamp, API source,
# endpoint, request URL, HTTP status and raw response body.
# The complete response is stored as text to preserve the original API output.
row = (
    ingest_ts,
    "cito",
    path,
    r.url,
    r.status_code,
    r.text
)

# Convert the row into a Spark DataFrame using the defined schema.
# Append it to the bronze table without overwriting existing records.
# This stores the entire API response in one row, even if it contains
# multiple events.
(
    spark.createDataFrame([row], bronze_schema)
    .write.mode("append")
    .saveAsTable("bronze_layer.dbo.bronze_cito_events")
)

# Query the bronze table to verify the ingested data.
# Show the ingestion timestamp, endpoint, HTTP status, payload length
# and first 40 characters of the raw response.
# Sort by ingestion timestamp so the latest records appear first.
display(spark.sql("""
    SELECT
        ingest_ts,
        endpoint,
        http_status,
        length(payload) AS payload_chars,
        left(payload, 40) AS payload_start
    FROM bronze_layer.dbo.bronze_cito_events
    ORDER BY ingest_ts DESC
"""))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Extract the event list from the existing API response.
events = r.json()["data"]

# Keep only events with statistics and a valid 16-character hexadecimal dataId.
filtered_events = [
    event for event in events
    if event.get("hasStats") is True
    and re.fullmatch(r"[0-9a-fA-F]{16}", str(event.get("dataId", "")))
]

# Print the slugs so we can verify the filter before making more API calls.
slugs = [event["slug"] for event in filtered_events]
print(f"Filtered events: {len(slugs)}")
print(*slugs, sep="\n")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.types import (StructType, StructField, StringType, IntegerType, TimestampType)

# Extend the existing bronze schema with event-specific metadata.
# Reuse the original fields and add the event slug and event date.
stats_schema = StructType(bronze_schema.fields + [
    StructField("event_slug", StringType(), True),
    StructField("event_date", StringType(), True)
])

# Collect all API responses in memory so they can be written in one batch.
rows = []

# Request statistics for each event that passed the filter.
for event in filtered_events:
    event_slug = event["slug"]
    event_date = event.get("eventDate")

    # Build the statistics endpoint for this event and capture the full response.
    path = f"/ufc/events/{event_slug}/stats"
    r_ev = call(path)

    # Store the original response body and metadata, including HTTP errors.
    # The two extra fields identify which event the response belongs to.
    rows.append((
        ingest_ts,
        "cito",
        path,
        r_ev.url,
        r_ev.status_code,
        r_ev.text,
        event_slug,
        event_date
    ))

    # Pause between requests to reduce the risk of exceeding API rate limits.
    time.sleep(7)

# Create one Spark DataFrame from all collected responses and append it to bronze.
if rows:
    (spark.createDataFrame(rows, stats_schema)
        .write.mode("append")
        .saveAsTable("bronze_layer.dbo.bronze_cito_event_stats"))

# Read back the latest records to verify the event identifiers, HTTP statuses
# and response payload sizes.
display(spark.sql("""
    SELECT ingest_ts, event_slug, event_date, http_status, length(payload) AS payload_chars
    FROM bronze_layer.dbo.bronze_cito_event_stats
    ORDER BY ingest_ts DESC
    
"""))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(spark.sql("""
    SELECT ingest_ts, count(*) AS n_rows, count_if(event_date IS NULL OR event_date = '') AS empty_dates
    FROM bronze_layer.dbo.bronze_cito_event_stats
    GROUP BY ingest_ts ORDER BY ingest_ts
"""))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for row in spark.table("bronze_layer.dbo.bronze_cito_event_stats").orderBy("ingest_ts", "event_slug").collect():
    d = json.loads(row.payload)["data"]
    print(row.ingest_ts, row.event_slug, row.event_date, "|", len(d["bouts"]), "bouts |", len(d["roundStats"]), "round-stat rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Read the registered Delta table created by saveAsTable.
df1 = spark.read.table("bronze_layer.dbo.bronze_cito_event_stats")

# Display the rows.
display(df1)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F

raw = spark.read.table("bronze_cito_event_stats")
one = raw.select("payload").limit(1)
schema_df = spark.read.json(one.rdd.map(lambda r: r.payload))
schema_df.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
