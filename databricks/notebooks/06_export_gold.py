# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Export Gold for the serving layer
# MAGIC Writes every Gold table and the dimensions as **one Parquet file each** plus `manifest.json`
# MAGIC (expected columns, row counts and control sums) to `landing/export/`. The serving loader refuses to load a
# MAGIC table that does not match the manifest. Run it after `05_backfill_late` + `04_reconcile (complete)`.
# MAGIC Then download the folder (Catalog → volume → `export` → download each file, or `databricks fs cp -r`).

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

from ai_spend import export as ex  # noqa: E402
from ai_spend import transform as tf  # noqa: E402
from ai_spend.ops import RunLog  # noqa: E402

base = f"/Volumes/{catalog}/{schema}/landing"
fq = lambda name: f"`{catalog}`.`{schema}`.{name}"  # noqa: E731

# COMMAND ----------

with RunLog(spark, fq("ops_run_log"), "export_gold", run_id) as log:
    dims = tf.load_dimensions(spark, f"{base}/dimensions")
    tables = {name: spark.table(fq(name)) for name in ex.GOLD_TABLES}
    tables.update(ex.dimension_frames(dims))
    manifest = ex.export_tables(tables, f"{base}/export", meta={"catalog": catalog, "schema": schema, "run_id": run_id})
    log.metrics.update({f"rows_{k}": v["rows"] for k, v in manifest["tables"].items()})
    print(f"exported {len(manifest['tables'])} tables to {base}/export")
    summary = [(k, v["rows"], v["file"]) for k, v in manifest["tables"].items()]
    display(spark.createDataFrame(summary, "table string, rows long, file string"))
