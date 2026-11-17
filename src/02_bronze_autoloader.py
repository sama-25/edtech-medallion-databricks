# Databricks notebook source
# MAGIC %md
# MAGIC ### 02 - Bronze: raw data, safely stored
# MAGIC - **Auto Loader** (`cloudFiles`) only picks up files it has not seen before (incremental).
# MAGIC - **Schema enforcement**: we give an explicit schema. Values that don't fit, or columns we
# MAGIC   didn't expect, are NOT lost - they land in `_rescued_data`.
# MAGIC - We add lineage columns: which file a row came from and when we ingested it.
# MAGIC - `availableNow` trigger = process everything new, then stop (cheap, job-friendly).

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql.types import *

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "edtech_medallion")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

landing = f"/Volumes/{catalog}/{schema}/landing/quiz_events"
checkpoint = f"/Volumes/{catalog}/{schema}/checkpoints/bronze_quiz_events"
bronze_table = f"{catalog}.{schema}.bronze_quiz_events"

event_schema = StructType([
    StructField("event_id", StringType()),
    StructField("student_id", StringType()),
    StructField("course_id", StringType()),
    StructField("question_id", StringType()),
    StructField("category", StringType()),
    StructField("difficulty", StringType()),
    StructField("is_correct", BooleanType()),
    StructField("time_taken_sec", DoubleType()),
    StructField("event_ts", StringType()),   # parsed to timestamp in Silver
    StructField("device", StringType()),
])

# COMMAND ----------
raw = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .option("rescuedDataColumn", "_rescued_data")
    .schema(event_schema)
    .load(landing)
    .select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )
)

(
    raw.writeStream
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(bronze_table)
    .awaitTermination()
)

print("Bronze rows:", spark.table(bronze_table).count())
