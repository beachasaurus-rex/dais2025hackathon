# Databricks notebook source
# MAGIC %md
# MAGIC # 🧠 Data Doctor - Agent Controller Notebook
# MAGIC This notebook loads pipeline failure data, identifies issue type, and calls subroutines for schema drift check, performance analysis, and fix recommendations.
# COMMAND ----------
import json
# Load mock failure data
with open("/dbfs/mnt/data/mock_dlt_failure.json", "r") as f:
   failure_event = json.load(f)
print("🚨 Pipeline Failure Detected:")
print(f"Pipeline ID: {failure_event['pipeline_id']}")
print(f"Timestamp: {failure_event['timestamp']}")
print(f"Error: {failure_event['cause']}")
# COMMAND ----------
# Simple rule-based triage to determine issue type
def classify_issue(error_text):
   if "Column" in error_text and "not found" in error_text:
       return "schema_drift"
   elif "shuffle" in error_text or "out of memory" in error_text:
       return "performance"
   else:
       return "unknown"
issue_type = classify_issue(failure_event["cause"])
print(f"🧪 Identified Issue Type: {issue_type}")
# COMMAND ----------
# === Branch: Schema Drift Analysis ===
if issue_type == "schema_drift":
   print("🔍 Running schema drift analyzer...")
   # Load schemas
   with open("/dbfs/mnt/data/schema_snapshot_earlier.json", "r") as f:
       schema_earlier = json.load(f)
   with open("/dbfs/mnt/data/schema_snapshot_latest.json", "r") as f:
       schema_latest = json.load(f)
   old_fields = {f['name'] for f in schema_earlier['fields']}
   new_fields = {f['name'] for f in schema_latest['fields']}
   added = new_fields - old_fields
   removed = old_fields - new_fields
   print("🟢 Added fields:", added)
   print("🔴 Removed fields:", removed)
   schema_issue_summary = {
       "type": "Schema Drift",
       "added_columns": list(added),
       "removed_columns": list(removed),
       "suggested_fix": f"Ensure downstream tables are updated to match new schema. Consider adding `customer_status` back or modifying target schema."
   }
   print("🛠️ Suggested Fix:", schema_issue_summary["suggested_fix"])
# COMMAND ----------
# === Branch: Query Performance Analysis ===
elif issue_type == "performance":
   print("🔍 Running query performance analyzer...")
   with open("/dbfs/mnt/data/query_performance_data.json", "r") as f:
       performance_data = json.load(f)
   for query in performance_data["queries"]:
       if query["duration_ms"] > 5000:
           print(f"⚠️ Query {query['query_id']} took {query['duration_ms']} ms")
           if "shuffle_read_mb" in query and query["shuffle_read_mb"] > 100:
               print(f"🔍 High shuffle detected: {query['shuffle_read_mb']} MB — consider repartitioning or broadcast joins")
# COMMAND ----------
# === Branch: Unknown Errors (fallback to LLM) ===
else:
   print("🤖 Unknown issue type — sending to LLM for classification and fix suggestion.")
   from openai import OpenAI
   import openai
   openai.api_key = dbutils.secrets.get("openai", "api_key")
   prompt = f"""
   A Databricks pipeline failed with the following error message:
   {failure_event['cause']}
   Please identify the cause and suggest a fix in plain English and SQL/PySpark code if relevant.
   """
   response = openai.ChatCompletion.create(
       model="gpt-4",
       messages=[{"role": "user", "content": prompt}]
   )
   print("🧠 LLM Response:")
   print(response['choices'][0]['message']['content'])
# COMMAND ----------
# MAGIC %md
# MAGIC ### ✅ Controller notebook complete — Data Doctor has delivered diagnosis and recommendation.