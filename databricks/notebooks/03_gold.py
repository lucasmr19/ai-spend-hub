# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — KPIs behind the publication gate
# MAGIC Evaluates the data-quality gate first. If it fails the notebook raises **before touching any Gold table**,
# MAGIC so dashboards keep showing the previous (trusted) version. Otherwise rebuilds all Gold tables from Silver.
# MAGIC Money is exact: integer `*_units` (1e-8 EUR) aggregated, converted to `DECIMAL(20,8)` at the edge.

# COMMAND ----------

import os
import sys
import uuid

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "ai_spend", "Schema")
dbutils.widgets.text("repo_src", "", "Path to databricks/src (blank = ../src)")
dbutils.widgets.text("run_id", "", "Run id (blank = generated)")
dbutils.widgets.text("min_valid_rate_pct", "97", "Gate: minimum valid rate %")
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

with RunLog(spark, fq("ops_run_log"), "gold", run_id) as log:
    dims = tf.load_dimensions(spark, f"{base}/dimensions")
    audit = spark.table(fq("silver_line_audit"))
    silver = spark.table(fq("silver_inference_events"))

    gate = evaluate_gate(audit, silver, min_valid_rate_pct=float(dbutils.widgets.get("min_valid_rate_pct")))
    display(spark.createDataFrame(gate))
    log.metrics["gate"] = gate
    enforce_gate(gate)                      # raises -> Gold untouched

    gd.register_views(spark, silver=silver, classified=audit, dims=dims)
    for name, df in gd.build_gold(spark).items():
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(fq(name))
        log.metrics[f"rows_{name}"] = spark.table(fq(name)).count()
    print(log.metrics)
