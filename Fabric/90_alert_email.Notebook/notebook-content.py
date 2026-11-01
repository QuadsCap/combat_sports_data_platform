# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# PARAMETERS CELL ********************

# PARAMETER CELL: the pipeline passes its run id; "manual" when run by hand.
RUN_ID = "manual"
PIPELINE = "pipeline_cito_refresh"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests

# Failure alert for pipeline_cito_refresh, sent as a push notification via ntfy.sh.
# Why not email: the Fabric account has no Exchange mailbox (no Outlook activity), and
# Gmail SMTP drops logins from Fabric Spark (2026-10-31). HTTPS works from Fabric (Cito API).
# The topic name is the only secret: anyone who knows it can read and post, so it lives in Key Vault.
KV = "https://combat-sports-vang-v1.vault.azure.net/"
topic = notebookutils.credentials.getSecret(KV, "alert-ntfy-topic")



r = requests.post(f"https://ntfy.sh/{topic}",
    data=f"Pipeline {PIPELINE}, run {RUN_ID} failed. Open Monitor in Fabric.".encode("utf-8"),
    headers={"Title": f"{PIPELINE} FAILED", "Priority": "high", "Tags": "warning"},
    timeout=30)
r.raise_for_status()   # a failed alert must fail this notebook too
print("alert sent for run", RUN_ID)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
