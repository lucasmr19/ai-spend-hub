# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — validate, deduplicate, route late data
# MAGIC Reads Bronze and classifies **every line** (`quarantine` / `duplicate` / `late_quarantine` / `silver`)
# MAGIC with the same logic that the local tests reconcile against `ground_truth.json`.
# MAGIC Writes use `MERGE ... WHEN NOT MATCHED INSERT`, so the notebook is idempotent: re-running adds nothing.
# MAGIC *First arrival wins*; duplicates are resolved **before** the 7-day watermark rule.

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

from ai_spend import transform as tf  # noqa: E402
from ai_spend.ops import RunLog  # noqa: E402

base = f"/Volumes/{catalog}/{schema}/landing"
fq = lambda name: f"`{catalog}`.`{schema}`.{name}"  # noqa: E731
log_table = fq("ops_run_log")

# COMMAND ----------

def create_if_missing(df, table):
    """Create an empty Delta table with df's schema if it does not exist yet."""
    if not spark.catalog.tableExists(table):
        df.limit(0).write.format("delta").saveAsTable(table)


def merge_insert_only(df, table, key):
    df.createOrReplaceTempView("_src")
    spark.sql(f"MERGE INTO {table} t USING _src s ON t.{key} = s.{key} WHEN NOT MATCHED THEN INSERT *")


# COMMAND ----------

with RunLog(spark, log_table, "silver", run_id) as log:
    dims = tf.load_dimensions(spark, f"{base}/dimensions")
    bronze = spark.table(fq("bronze_inference_events"))
    # Serverless compute does not support .cache()/.persist(): materialise the classification once in a
    # staging Delta table (overwritten every run, a deterministic function of Bronze) and read from it.
    stage = fq("_stg_classified")
    (tf.classify_lines(bronze, dims).write.format("delta").mode("overwrite")
        .option("overwriteSchema", "true").saveAsTable(stage))
    classified = spark.table(stage)

    silver_new = tf.canonicalize(classified, "silver")
    late_new = tf.canonicalize(classified, "late_quarantine")
    quarantine_new = tf.quarantine_rows(classified)

    t_silver, t_late, t_quar = fq("silver_inference_events"), fq("silver_late_quarantine"), fq("silver_quarantine")
    for df, t in [(silver_new, t_silver), (late_new, t_late), (quarantine_new, t_quar)]:
        create_if_missing(df, t)

    # a late event whose id is already in Silver must not be re-quarantined
    late_new = late_new.join(spark.table(t_silver).select("event_id"), "event_id", "left_anti")
    merge_insert_only(silver_new, t_silver, "event_id")
    merge_insert_only(late_new, t_late, "event_id")
    merge_insert_only(quarantine_new, t_quar, "quarantine_key")

    # per-line audit (deterministic function of Bronze -> overwritten each run, idempotent)
    (classified.select("source_file", "arrival_date", "schema_version", "event_id", "reason", "disposition", "lag_days")
        .write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq("silver_line_audit")))

    by_disp = classified.groupBy("disposition").count().withColumnRenamed("count", "n")
    counts = {r["disposition"]: r["n"] for r in by_disp.collect()}
    log.metrics.update(lines=sum(counts.values()), **counts, silver_rows=spark.table(t_silver).count(),
                       late_quarantine_rows=spark.table(t_late).count(), quarantine_rows=spark.table(t_quar).count())
    print(log.metrics)
