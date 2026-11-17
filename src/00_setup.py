# Databricks notebook source
# MAGIC %md
# MAGIC ### 00 - Setup
# MAGIC Creates the schema and two Unity Catalog Volumes:
# MAGIC `landing` (where raw files arrive) and `checkpoints` (the "bookmarks" for streaming).

# COMMAND ----------
dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "edtech_medallion")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
for vol in ["landing", "checkpoints"]:
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{vol}")

print(f"Ready: {catalog}.{schema}")
