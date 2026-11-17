# Databricks notebook source
# MAGIC %md
# MAGIC ### 03 - Silver: cleaned, trusted data
# MAGIC - Streams only NEW Bronze rows (checkpoint = bookmark).
# MAGIC - Splits each micro-batch into **good** rows and **bad** rows (quarantine table).
# MAGIC - **MERGE on event_id** -> safe to re-run, duplicates can never sneak in (idempotent).
# MAGIC - Delta CHECK constraints are a second safety net at the table level.

# COMMAND ----------
from pyspark.sql import functions as F
from delta.tables import DeltaTable

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "edtech_medallion")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

bronze_table = f"{catalog}.{schema}.bronze_quiz_events"
silver_table = f"{catalog}.{schema}.silver_quiz_events"
quarantine_table = f"{catalog}.{schema}.silver_quarantine"
checkpoint = f"/Volumes/{catalog}/{schema}/checkpoints/silver_quiz_events"

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {silver_table} (
  event_id STRING NOT NULL,
  student_id STRING NOT NULL,
  course_id STRING,
  question_id STRING,
  category STRING,
  difficulty STRING,
  is_correct BOOLEAN,
  time_taken_sec DOUBLE,
  event_ts TIMESTAMP,
  event_date DATE,
  device STRING,
  _source_file STRING,
  _ingested_at TIMESTAMP,
  CONSTRAINT non_negative_time CHECK (time_taken_sec >= 0),
  CONSTRAINT valid_difficulty CHECK (difficulty IN ('easy','medium','hard'))
) USING DELTA
""")

SILVER_COLS = ["event_id", "student_id", "course_id", "question_id", "category", "difficulty",
               "is_correct", "time_taken_sec", "event_ts", "event_date", "device",
               "_source_file", "_ingested_at"]

# COMMAND ----------
def process_batch(batch_df, batch_id):
    parsed = (
        batch_df
        .withColumn("difficulty", F.lower(F.trim("difficulty")))
        .withColumn("event_ts_parsed", F.to_timestamp("event_ts"))
    )

    is_valid = F.coalesce(
        F.col("event_id").isNotNull()
        & F.col("student_id").isNotNull()
        & F.col("event_ts_parsed").isNotNull()
        & F.col("is_correct").isNotNull()
        & (F.col("time_taken_sec") >= 0)
        & F.col("difficulty").isin("easy", "medium", "hard"),
        F.lit(False),
    )

    good = (
        parsed.filter(is_valid)
        .withColumn("event_ts", F.col("event_ts_parsed"))
        .withColumn("event_date", F.to_date("event_ts"))
        .dropDuplicates(["event_id"])
        .select(*SILVER_COLS)
    )
    bad = (
        parsed.filter(~is_valid)
        .withColumn("_reason", F.lit("failed_validation"))
        .withColumn("_quarantined_at", F.current_timestamp())
        .drop("event_ts_parsed")
    )

    (DeltaTable.forName(spark, silver_table).alias("t")
        .merge(good.alias("s"), "t.event_id = s.event_id")
        .whenNotMatchedInsertAll()
        .execute())

    bad.write.mode("append").option("mergeSchema", "true").saveAsTable(quarantine_table)

# COMMAND ----------
(
    spark.readStream.table(bronze_table)
    .writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .start()
    .awaitTermination()
)

print("Silver rows:", spark.table(silver_table).count())
print("Quarantined rows:", spark.table(quarantine_table).count() if spark.catalog.tableExists(quarantine_table) else 0)
