from pyspark.sql.functions import col
# Load latest schema
df_latest = spark.read.table("your_catalog.schema.target_table")
schema_latest = df_latest.schema
# Load earlier snapshot (or saved schema)
# Simulate by reading from a stored Delta version or JSON
df_earlier = spark.read.option("versionAsOf", 5).table("your_catalog.schema.target_table")
schema_earlier = df_earlier.schema
# Compare fields
fields_added = set(f.name for f in schema_latest) - set(f.name for f in schema_earlier)
fields_removed = set(f.name for f in schema_earlier) - set(f.name for f in schema_latest)
print("🟢 Added fields:", fields_added)
print("🔴 Removed fields:", fields_removed)