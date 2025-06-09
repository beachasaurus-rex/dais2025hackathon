import requests
import json
import os
# Config
DATABRICKS_INSTANCE = "https://<your-workspace>.cloud.databricks.com"
TOKEN = dbutils.secrets.get("databricks", "PAT")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
# Replace with your actual pipeline ID
pipeline_id = "<DLT_pipeline_id>"
# Get latest pipeline status
response = requests.get(
   f"{DATABRICKS_INSTANCE}/api/2.0/pipelines/{pipeline_id}/updates",
   headers=HEADERS
)
updates = response.json()["updates"]
latest_failed = next((u for u in updates if u["state"] == "FAILED"), None)
if latest_failed:
   update_id = latest_failed["update_id"]
   logs = latest_failed.get("cause", "No cause provided")
   print(f"❌ Pipeline failed with ID {update_id}:\n{logs}")
else:
   print("✅ No recent failures found.")