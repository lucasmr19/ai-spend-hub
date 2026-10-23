"""Export Gold + dimensions as single-file Parquet with a manifest (the hand-off to the serving layer).

The manifest is the contract between Gold and the serving warehouse: expected columns/types, row
counts and control sums per table. The loader refuses to load a table that does not match it.
"""
from __future__ import annotations

import glob
import json
import os
import shutil
from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# Columns whose SUM is verified after loading (exact: integers or decimals only)
CONTROL_SUMS: dict[str, list[str]] = {
    "fact_inference": ["cost_units", "tokens_in", "tokens_out"],
    "gold_cost_daily": ["requests", "cost_units", "cache_savings_units"],
    "gold_budget_vs_actual": ["cost_units"],
    "gold_slo_monthly": ["total", "failed"],
    "gold_model_performance": ["invocations", "cost_units"],
    "gold_dq_daily": ["lines_received", "accepted", "duplicates_dropped", "late_quarantined", "invalid_quarantined"],
    "gold_quarantine_summary": ["records"],
    "dim_departments": [],
    "dim_apps": [],
    "dim_models": [],
    "dim_model_quality": ["eval_samples"],
    "dim_budgets": ["budget_eur"],
    "dim_slo_targets": [],
}

GOLD_TABLES = [t for t in CONTROL_SUMS if not t.startswith("dim_")]
DIM_TABLES = [t for t in CONTROL_SUMS if t.startswith("dim_")]


def dimension_frames(dims: dict[str, DataFrame]) -> dict[str, DataFrame]:
    return {f"dim_{k}": dims[k] for k in ["departments", "apps", "models", "model_quality", "budgets", "slo_targets"]}


def export_tables(tables: dict[str, DataFrame], out_dir: str, *, meta: dict | None = None) -> dict:
    """Write ``<out_dir>/<table>.parquet`` (one file each) and ``manifest.json``. Returns the manifest."""
    os.makedirs(out_dir, exist_ok=True)
    manifest: dict = {"generated_at": datetime.now(timezone.utc).isoformat(), "meta": meta or {}, "tables": {}}
    for name, df in tables.items():
        tmp = os.path.join(out_dir, f"_tmp_{name}")
        df.coalesce(1).write.mode("overwrite").parquet(tmp)
        part = sorted(glob.glob(os.path.join(tmp, "part-*.parquet")))
        if len(part) != 1:
            raise RuntimeError(f"expected one parquet part for {name}, found {part}")
        target = os.path.join(out_dir, f"{name}.parquet")
        if os.path.exists(target):
            os.remove(target)
        shutil.move(part[0], target)
        shutil.rmtree(tmp, ignore_errors=True)

        sums = CONTROL_SUMS.get(name, [])
        row = df.agg(F.count("*").alias("rows"), *[F.sum(c).cast("string").alias(c) for c in sums]).first()
        manifest["tables"][name] = {
            "file": f"{name}.parquet",
            "rows": int(row["rows"]),
            "sums": {c: row[c] for c in sums},
            "columns": [{"name": n, "type": t} for n, t in df.dtypes],
        }
    with open(os.path.join(out_dir, "manifest.json"), "w") as f:
        json.dump(manifest, f, indent=2)
    return manifest
