# Databricks notebook source
# MAGIC %md
# MAGIC ### 04 - Gold: business-ready tables
# MAGIC Three tables a teacher / product manager / dashboard can use directly:
# MAGIC 1. `gold_student_progress`   - how is each student doing per course?
# MAGIC 2. `gold_course_daily`       - daily engagement and accuracy per course
# MAGIC 3. `gold_hardest_questions`  - which questions trip students up most?

# COMMAND ----------
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "edtech_medallion")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

silver = spark.table(f"{catalog}.{schema}.silver_quiz_events")
assert silver.count() > 0, "Silver is empty - nothing to aggregate"

correct = F.col("is_correct").cast("int")

# COMMAND ----------
student_progress = (
    silver.groupBy("student_id", "course_id")
    .agg(
        F.count("*").alias("attempts"),
        F.round(F.avg(correct) * 100, 1).alias("accuracy_pct"),
        F.round(F.avg("time_taken_sec"), 1).alias("avg_time_sec"),
        F.max("event_ts").alias("last_active_ts"),
    )
)
student_progress.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_student_progress")

# COMMAND ----------
course_daily = (
    silver.groupBy("course_id", "category", "event_date")
    .agg(
        F.countDistinct("student_id").alias("active_students"),
        F.count("*").alias("attempts"),
        F.round(F.avg(correct) * 100, 1).alias("accuracy_pct"),
    )
)
course_daily.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_course_daily")

# COMMAND ----------
hardest = (
    silver.groupBy("question_id", "category", "difficulty")
    .agg(
        F.count("*").alias("attempts"),
        F.round(F.avg(correct) * 100, 1).alias("accuracy_pct"),
        F.round(F.avg("time_taken_sec"), 1).alias("avg_time_sec"),
    )
    .filter("attempts >= 5")
    .orderBy("accuracy_pct")
)
hardest.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_hardest_questions")

display(spark.table(f"{catalog}.{schema}.gold_hardest_questions").limit(10))
