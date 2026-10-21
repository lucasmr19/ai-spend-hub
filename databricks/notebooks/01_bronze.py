# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — Auto Loader
# MAGIC Incrementally ingests every landed JSONL line **as raw text** with its source file and arrival date.
# MAGIC Nothing is parsed or rejected here: Bronze is the immutable audit copy. The checkpoint makes the job
# MAGIC idempotent — re-running it without new files does nothing.

# COMMAND ----------

import os
import sys
import uuid

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "ai_spend", "Schema")
dbutils.widgets.text("repo_src", "", "Path to databricks/src (blank = ../src)")
dbutils.widgets.text("run_id", "", "Run id (blank = generated)")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())
sys.path.insert(0, dbutils.widgets.get("repo_src") or os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from ai_spend.ops import RunLog  # noqa: E402
from ai_spend.transform import arrival_date_from_path  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

base = f"/Volumes/{catalog}/{schema}/landing"
table = f"`{catalog}`.`{schema}`.bronze_inference_events"
log_table = f"`{catalog}`.`{schema}`.ops_run_log"

# COMMAND ----------

with RunLog(spark, log_table, "bronze", run_id) as log:
    stream = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .load(f"{base}/inference_events")
        .select(
            F.col("value").alias("raw"),
            F.col("_metadata.file_path").alias("source_file"),
            arrival_date_from_path(F.col("_metadata.file_path")).alias("arrival_date"),
            F.current_timestamp().alias("ingested_at"),
        )
    )
    before = spark.table(table).count() if spark.catalog.tableExists(table) else 0
    (stream.writeStream
        .option("checkpointLocation", f"{base}/_checkpoints/bronze")
        .trigger(availableNow=True)           # serverless-friendly: process what is there, then stop
        .toTable(table)
        .awaitTermination())
    after = spark.table(table).count()
    log.metrics.update(rows_before=before, rows_after=after, rows_new=after - before)
    print(log.metrics)
