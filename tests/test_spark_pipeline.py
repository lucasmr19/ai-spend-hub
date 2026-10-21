"""Spark logic vs ground truth and vs the pure-Python oracle (runs on a local Spark session)."""
import json

import pytest

pytest.importorskip("pyspark")

from ai_spend import gold as gd  # noqa: E402
from ai_spend import transform as tf  # noqa: E402
from ai_spend.reconcile import assert_all, reconcile  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from generator.generate import run  # noqa: E402


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    s = (SparkSession.builder.master("local[2]").appName("ai-spend-tests")
         .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
         .config("spark.sql.shuffle.partitions", "4").config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").config("spark.driver.memory", "2g").getOrCreate())
    yield s
    s.stop()


@pytest.fixture(scope="module")
def data(tmp_path_factory):
    out = tmp_path_factory.mktemp("synthetic")
    truth = run(out, events_per_day=800, seed=42, preset="small")
    return out, truth


@pytest.fixture(scope="module")
def prepared(spark, data):
    out, _ = data
    dims = tf.load_dimensions(spark, str(out / "dimensions"))
    bronze = tf.read_bronze_local(spark, str(out / "landing" / "inference_events"))
    classified = tf.classify_lines(bronze, dims).cache()
    return dims, classified


def _gold(spark, dims, classified, silver):
    gd.register_views(spark, silver=silver, classified=classified, dims=dims)
    return gd.build_gold(spark)


def test_spark_reason_codes_match_python_oracle_line_by_line(spark, data, prepared):
    from datetime import date

    from contracts.validate import check_record

    out, _ = data
    _, classified = prepared
    spark_reasons = {(r["source_file"].split("/")[-2] + "|" + r["raw"]): r["reason"]
                     for r in classified.select("source_file", "raw", "reason").collect()}
    dims_dir = out / "dimensions"
    import csv

    models = {r["model_id"] for r in csv.DictReader(open(dims_dir / "models.csv"))}
    app_dept = {r["app_id"]: r["department_id"] for r in csv.DictReader(open(dims_dir / "apps.csv"))}
    checked = 0
    for path in sorted((out / "landing" / "inference_events").glob("dt=*/*")):
        arrival = date.fromisoformat(path.parent.name.split("=")[1])
        for line in path.read_text().splitlines():
            expected = check_record(json.loads(line), arrival, models, app_dept)
            assert spark_reasons[f"{path.parent.name}|{line}"] == expected, line
            checked += 1
    assert checked > 50_000


def test_pre_backfill_reconciles_with_ground_truth(spark, data, prepared):
    _, truth = data
    dims, classified = prepared
    silver = tf.canonicalize(classified, "silver")
    gold = _gold(spark, dims, classified, silver)
    assert_all(reconcile(truth=truth, gold=gold, classified=classified, silver=silver, phase="pre_backfill"))


def test_after_backfill_reconciles_exactly_with_ground_truth(spark, data, prepared):
    _, truth = data
    dims, classified = prepared
    silver = tf.canonicalize(classified, "silver").unionByName(tf.canonicalize(classified, "late_quarantine"))
    gold = _gold(spark, dims, classified, silver)
    checks = reconcile(truth=truth, gold=gold, classified=classified, silver=silver, phase="complete")
    assert_all(checks)
    assert len(checks) >= 14


def test_duplicates_are_resolved_before_the_watermark_rule(prepared):
    _, classified = prepared
    first = classified.filter("disposition IN ('silver','late_quarantine')")
    dups = classified.filter("disposition = 'duplicate'")
    # a line is silver XOR late_quarantine per event id, and every duplicate has exactly one first arrival
    assert first.groupBy("event_id").count().filter("count > 1").count() == 0
    joined = dups.alias("d").join(first.alias("f"), "event_id")
    assert joined.count() == dups.count()
    assert joined.filter(F.col("d.arrival_date") < F.col("f.arrival_date")).count() == 0


def test_dq_gold_tables_are_consistent(spark, data, prepared):
    _, truth = data
    dims, classified = prepared
    silver = tf.canonicalize(classified, "silver")
    gold = _gold(spark, dims, classified, silver)
    dqd = gold["gold_dq_daily"].agg(F.sum("lines_received").alias("n"), F.sum("duplicates_dropped").alias("d")).first()
    assert dqd["n"] == truth["dq"]["lines_emitted"] and dqd["d"] == truth["dq"]["duplicate_copies"]
    q = {r["reason_code"]: r["records"] for r in gold["gold_quarantine_summary"].collect()}
    assert q == truth["dq"]["invalid_by_code"]


def test_gate_passes_on_clean_data_and_blocks_a_poisoned_batch(spark, prepared):
    """AC-08: a corrupted batch must stop publication."""
    from ai_spend.gate import GateFailed, enforce_gate, evaluate_gate

    dims, classified = prepared
    silver = tf.canonicalize(classified, "silver")
    enforce_gate(evaluate_gate(classified, silver))  # clean data: no exception

    poison = spark.createDataFrame([("not json at all",)] * 3000, "raw string").select(
        "raw", F.lit("/v/dt=2026-10-25_bad.jsonl").alias("source_file"), F.lit("2026-10-25").cast("date").alias("arrival_date"))
    bad = tf.classify_lines(poison, dims)
    both = classified.unionByName(bad, allowMissingColumns=True)
    results = evaluate_gate(both, silver)
    assert not [r for r in results if r["id"] == "G1"][0]["ok"]
    with pytest.raises(GateFailed):
        enforce_gate(results)


def test_run_log_records_success_and_failure(spark):
    from ai_spend.ops import RunLog

    with RunLog(spark, "run_log_test", "t_ok", "r1") as log:
        log.metrics["rows"] = 5
    with pytest.raises(ValueError), RunLog(spark, "run_log_test", "t_bad", "r1"):
        raise ValueError("boom")
    rows = {r["task"]: r for r in spark.table("run_log_test").collect()}
    assert rows["t_ok"]["status"] == "SUCCESS" and '"rows": 5' in rows["t_ok"]["metrics"]
    assert rows["t_bad"]["status"] == "FAILED" and "boom" in rows["t_bad"]["error"]


def test_flat_layout_gives_same_classification(spark, data, tmp_path):
    from datetime import date as d

    kw = dict(events_per_day=300, seed=7, start=d(2026, 8, 10), end=d(2026, 8, 13))
    a, b = tmp_path / "a", tmp_path / "b"
    ta = run(a, **kw)
    tb = run(b, layout="flat", **kw)
    assert ta["dq"] == tb["dq"]
    dims = tf.load_dimensions(spark, str(b / "dimensions"))
    flat = tf.classify_lines(tf.read_bronze_local(spark, str(b / "landing" / "inference_events")), dims)
    part = tf.classify_lines(tf.read_bronze_local(spark, str(a / "landing" / "inference_events")), dims)
    assert {r[0]: r[1] for r in flat.groupBy("disposition").count().collect()} == \
        {r[0]: r[1] for r in part.groupBy("disposition").count().collect()}
