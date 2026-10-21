# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Reconcile against ground truth (AC-01 … AC-07)
# MAGIC `phase = pre_backfill`: very-late events are still in late quarantine, so costs equal the truth minus their cost.
# MAGIC `phase = complete`: after `05_backfill_late`, everything must match exactly.
# MAGIC Results are stored in `ops_reconciliation`; the notebook fails if any check fails.

# COMMAND ----------

import json
import os
import sys
import uuid

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "ai_spend", "Schema")
dbutils.widgets.text("repo_src", "", "Path to databricks/src (blank = ../src)")
dbutils.widgets.text("run_id", "", "Run id (blank = generated)")
dbutils.widgets.dropdown("phase", "pre_backfill", ["pre_backfill", "complete"], "Phase")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")
phase = dbutils.widgets.get("phase")
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())
sys.path.insert(0, dbutils.widgets.get("repo_src") or os.path.abspath(os.path.join(os.getcwd(), "..", "src")))

from ai_spend.ops import RunLog  # noqa: E402
from ai_spend.reconcile import assert_all, reconcile  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

base = f"/Volumes/{catalog}/{schema}/landing"
fq = lambda name: f"`{catalog}`.`{schema}`.{name}"  # noqa: E731

# COMMAND ----------

with RunLog(spark, fq("ops_run_log"), f"reconcile_{phase}", run_id) as log:
    truth = json.load(open(f"{base}/ground_truth.json"))
    audit = spark.table(fq("silver_line_audit"))
    silver = spark.table(fq("silver_inference_events"))
    gold_names = ["fact_inference", "gold_cost_daily", "gold_budget_vs_actual", "gold_slo_monthly",
                  "gold_model_performance", "gold_dq_daily", "gold_quarantine_summary"]
    gold = {n: spark.table(fq(n)) for n in gold_names}

    checks = reconcile(truth=truth, gold=gold, classified=audit, silver=silver, phase=phase)
    result = spark.createDataFrame(checks).withColumn("phase", F.lit(phase)) \
        .withColumn("run_id", F.lit(run_id)).withColumn("checked_at", F.current_timestamp())
    result.write.format("delta").mode("append").saveAsTable(fq("ops_reconciliation"))
    display(result)
    log.metrics.update(checks=len(checks), failed=sum(not c["ok"] for c in checks))
    assert_all(checks)
