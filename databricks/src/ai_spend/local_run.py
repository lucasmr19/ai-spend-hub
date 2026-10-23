"""Run Bronze -> Silver -> Gold -> export on a LOCAL Spark session (no Databricks needed).

    python -m ai_spend.local_run out/synthetic out/export

Uses the exact same functions as the notebooks. Handy to try the serving layer without a workspace.
"""
from __future__ import annotations

import sys

from pyspark.sql import DataFrame, SparkSession

from . import export as ex
from . import gold as gd
from . import transform as tf


def build_all(spark: SparkSession, root: str, *, backfill: bool = True) -> dict:
    """Return dims, classified, silver and gold DataFrames for a generated dataset directory."""
    dims = tf.load_dimensions(spark, f"{root}/dimensions")
    bronze = tf.read_bronze_local(spark, f"{root}/landing/inference_events")
    classified = tf.classify_lines(bronze, dims)
    silver: DataFrame = tf.canonicalize(classified, "silver")
    if backfill:
        silver = silver.unionByName(tf.canonicalize(classified, "late_quarantine"))
    gd.register_views(spark, silver=silver, classified=classified, dims=dims)
    return {"dims": dims, "classified": classified, "silver": silver, "gold": gd.build_gold(spark)}


def local_session(app: str = "ai-spend-local") -> SparkSession:
    return (SparkSession.builder.master("local[2]").appName(app)
            .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false").config("spark.driver.memory", "2g").getOrCreate())


def main(root: str, out_dir: str) -> None:
    spark = local_session()
    built = build_all(spark, root)
    tables = {**built["gold"], **ex.dimension_frames(built["dims"])}
    manifest = ex.export_tables(tables, out_dir, meta={"source": "local_run", "root": root})
    for name, t in manifest["tables"].items():
        print(f"{name:26} {t['rows']:>9,} rows")
    spark.stop()


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
