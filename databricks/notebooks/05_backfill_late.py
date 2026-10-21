# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Backfill very-late events
# MAGIC Events that arrive more than 7 days after their event date are held in `silver_late_quarantine`
# MAGIC (they would silently change already-published months). This documented job applies them to Silver and
# MAGIC rebuilds Gold, so the published numbers change only through a visible, logged operation.
# MAGIC Idempotent: `MERGE ... WHEN NOT MATCHED`.

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

from ai_spend import gold as gd  # noqa: E402
from ai_spend import transform as tf  # noqa: E402
from ai_spend.gate import enforce_gate, evaluate_gate  # noqa: E402
from ai_spend.ops import RunLog  # noqa: E402

base = f"/Volumes/{catalog}/{schema}/landing"
fq = lambda name: f"`{catalog}`.`{schema}`.{name}"  # noqa: E731

# COMMAND ----------

with RunLog(spark, fq("ops_run_log"), "backfill_late", run_id) as log:
    late = spark.table(fq("silver_late_quarantine"))
    before = spark.table(fq("silver_inference_events")).count()
    late.createOrReplaceTempView("_late")
    spark.sql(f"""MERGE INTO {fq('silver_inference_events')} t USING _late s ON t.event_id = s.event_id
                  WHEN NOT MATCHED THEN INSERT *""")
    silver = spark.table(fq("silver_inference_events"))
    after = silver.count()
    log.metrics.update(late_rows=late.count(), silver_before=before, silver_after=after, applied=after - before)

    dims = tf.load_dimensions(spark, f"{base}/dimensions")
    audit = spark.table(fq("silver_line_audit"))
    enforce_gate(evaluate_gate(audit, silver))
    gd.register_views(spark, silver=silver, classified=audit, dims=dims)
    for name, df in gd.build_gold(spark).items():
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq(name))
    print(log.metrics)
