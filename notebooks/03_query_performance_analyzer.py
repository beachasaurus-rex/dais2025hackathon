# For Delta tables
history_df = spark.sql("DESCRIBE HISTORY your_catalog.schema.table")
history_df.select("timestamp", "operationMetrics").show(truncate=False)
# Or, if you have Spark event logs registered
event_logs_df = spark.read.json("/databricks/spark-events/")
event_logs_df.filter("Event == 'SparkListenerJobEnd'").select("Job ID", "Completion Time").show()