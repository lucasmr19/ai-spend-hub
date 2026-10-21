# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — schema, volume and landing check
# MAGIC Creates the schema and a Unity Catalog **volume** used as landing area, then checks that the
# MAGIC synthetic files were uploaded (see `docs/databricks-runbook.md`, section 2).
# MAGIC Safe to re-run (idempotent).

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace", "Catalog")
dbutils.widgets.text("schema", "ai_spend", "Schema")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
spark.sql(f"CREATE VOLUME IF NOT EXISTS `{catalog}`.`{schema}`.`landing`")
base = f"/Volumes/{catalog}/{schema}/landing"
print("landing volume:", base)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Expected layout inside the volume
# MAGIC ```
# MAGIC landing/
# MAGIC   inference_events/   dt=YYYY-MM-DD/events-000.jsonl   (or flat: dt=YYYY-MM-DD_events-000.jsonl)
# MAGIC   dimensions/         departments.csv apps.csv models.csv model_prices.csv slo_targets.csv model_quality.csv budgets.csv
# MAGIC   ground_truth.json
# MAGIC ```

# COMMAND ----------

import os  # noqa: E402

problems = []
for rel in ["dimensions/models.csv", "dimensions/apps.csv", "dimensions/model_prices.csv",
            "dimensions/slo_targets.csv", "dimensions/model_quality.csv", "dimensions/budgets.csv",
            "dimensions/departments.csv", "ground_truth.json"]:
    if not os.path.exists(f"{base}/{rel}"):
        problems.append(rel)
events_dir = f"{base}/inference_events"
n_files = sum(len(f) for _, _, f in os.walk(events_dir)) if os.path.exists(events_dir) else 0
print(f"event files found: {n_files}")
if problems or n_files == 0:
    raise RuntimeError(f"Landing area incomplete. Missing: {problems or 'no event files'}. "
                       "Upload them first (runbook section 2).")
print("OK: landing area is ready")
