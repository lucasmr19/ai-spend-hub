"""Reconcile Gold/Silver against ``ground_truth.json`` (acceptance criteria AC-01 .. AC-07).

``phase``:
    "pre_backfill"  very-late events (> watermark) are still in late quarantine
    "complete"      very-late events were applied by the backfill job
"""
from __future__ import annotations

from decimal import Decimal

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

UNIT = Decimal(10) ** 8


def _eur_str(units: int) -> str:
    return f"{Decimal(units) / UNIT:.8f}"


def _check(checks: list[dict], cid: str, name: str, ok: bool, detail: str = "") -> None:
    checks.append({"id": cid, "name": name, "ok": bool(ok), "detail": "" if ok else detail})


def reconcile(*, truth: dict, gold: dict[str, DataFrame], classified: DataFrame, silver: DataFrame,
              phase: str = "complete") -> list[dict]:
    checks: list[dict] = []
    dq = truth["dq"]

    # ---- AC-01 cost per department-month ------------------------------------------------
    rows = (gold["gold_cost_daily"].groupBy("department_id", F.date_format("event_date", "yyyy-MM").alias("m"))
            .agg(F.sum("cost_units").alias("u")).collect())
    got = {(r["department_id"], r["m"]): int(r["u"]) for r in rows}
    bad = []
    for dept, months in truth["cost_by_department_month_eur"].items():
        for m, expected in months.items():
            exp_units = int(Decimal(expected) * UNIT)
            if phase == "pre_backfill":
                exp_units -= int(Decimal(truth["very_late_cost_by_department_month_eur"][dept][m]) * UNIT)
            if got.get((dept, m), 0) != exp_units:
                bad.append((dept, m, _eur_str(got.get((dept, m), 0)), _eur_str(exp_units)))
    _check(checks, "AC-01", f"cost per department-month ({phase})", not bad, f"mismatches: {bad[:3]}")

    # ---- AC-02 duplicates ---------------------------------------------------------------
    dup_ids = silver.groupBy("event_id").count().filter("count > 1").count()
    dropped = classified.filter(F.col("disposition") == "duplicate").count()
    _check(checks, "AC-02", "no duplicate event_id in Silver", dup_ids == 0, f"{dup_ids} duplicated ids")
    _check(checks, "AC-02b", "duplicates dropped == ground truth", dropped == dq["duplicate_copies"],
           f"{dropped} vs {dq['duplicate_copies']}")

    # ---- AC-03 quarantine by reason code --------------------------------------------------
    inv = {r["reason"]: r["n"] for r in classified.filter("disposition = 'quarantine'")
           .groupBy("reason").agg(F.count("*").alias("n")).collect()}
    _check(checks, "AC-03", "quarantine counts per reason code", inv == dq["invalid_by_code"],
           f"{inv} vs {dq['invalid_by_code']}")

    # ---- AC-04 schema versions ------------------------------------------------------------
    ver = {f"v{r['schema_version']}": r["n"] for r in classified.groupBy("schema_version")
           .agg(F.count("*").alias("n")).collect()}
    _check(checks, "AC-04", "v1 and v2 lines processed together", ver == dq["lines_by_schema_version"],
           f"{ver} vs {dq['lines_by_schema_version']}")
    canon_versions = {r[0] for r in silver.select("source_schema_version").distinct().collect()}
    _check(checks, "AC-04b", "Silver holds both source versions in one schema", canon_versions == {1, 2},
           str(canon_versions))

    # ---- AC-05 late data ------------------------------------------------------------------
    late_q = classified.filter("disposition = 'late_quarantine'").count()
    very_late_expected = dq["very_late_events_gt_7d"]
    if phase == "pre_backfill":
        _check(checks, "AC-05", "very-late events held in late quarantine", late_q == very_late_expected,
               f"{late_q} vs {very_late_expected}")
    else:
        in_silver_late = silver.filter(F.col("lag_days") > 7).count()
        _check(checks, "AC-05", "very-late events applied by backfill", in_silver_late == very_late_expected,
               f"{in_silver_late} vs {very_late_expected}")
    n_unique = silver.count() + (late_q if phase == "pre_backfill" else 0)
    _check(checks, "AC-05b", "unique valid events == ground truth", n_unique == dq["unique_valid_events"],
           f"{n_unique} vs {dq['unique_valid_events']}")

    if phase != "complete":
        return checks

    # ---- AC-01b cost per model-month and per app-month ---------------------------------------
    rows = gold["gold_cost_daily"].groupBy("model_id", F.date_format("event_date", "yyyy-MM").alias("m")) \
        .agg(F.sum("cost_units").alias("u")).collect()
    got_m = {(r["model_id"], r["m"]): int(r["u"]) for r in rows}
    bad = [(mo, m) for mo, ms in truth["cost_by_model_month_eur"].items() for m, e in ms.items()
           if got_m.get((mo, m), 0) != int(Decimal(e) * UNIT)]
    _check(checks, "AC-01b", "cost per model-month", not bad, str(bad[:3]))

    # ---- AC-01c budgets: which departments are over budget ------------------------------------
    rows = gold["gold_budget_vs_actual"].filter("over_budget").select("department_id", "month").collect()
    got_over = {(r[0], r[1]) for r in rows}
    exp_over = {(d, m) for d, ms in truth["budget_status_by_department_month"].items()
                for m, x in ms.items() if x["over_budget"]}
    _check(checks, "AC-01c", "departments over budget", got_over == exp_over, f"{got_over} vs {exp_over}")

    # ---- AC-06 availability -------------------------------------------------------------------
    rows = gold["gold_slo_monthly"].collect()
    bad = []
    for r in rows:
        x = truth["slo_by_app_month"][r["app_id"]][r["month"]]
        if (r["total"], r["failed"]) != (x["total"], x["failed"]) or bool(r["breached"]) != x["breached"] \
                or abs(r["availability_pct"] - x["availability_pct"]) > 0.001:
            bad.append((r["app_id"], r["month"]))
    breached = sorted((r["app_id"], r["month"]) for r in rows if r["breached"])
    _check(checks, "AC-06", "availability per app-month and breaches", not bad, str(bad[:3]))
    _check(checks, "AC-06b", "SLO breaches flagged", breached == [("contract-review", "2026-08")], str(breached))

    # ---- AC-07 cache savings ----------------------------------------------------------------------
    rows = gold["gold_cost_daily"].groupBy(F.date_format("event_date", "yyyy-MM").alias("m")) \
        .agg(F.sum("cache_savings_units").alias("u")).collect()
    got_c = {r["m"]: int(r["u"]) for r in rows}
    bad = [m for m, x in truth["cache_by_month"].items() if got_c.get(m, 0) != int(Decimal(x["savings_eur"]) * UNIT)]
    _check(checks, "AC-07", "cache savings per month", not bad, str(bad))

    # ---- KPI-2 overall model performance -----------------------------------------------------------
    rows = {r["model_id"]: r for r in gold["fact_inference"].filter("is_billable").groupBy("model_id").agg(
        F.count("*").alias("n"), F.sum("cost_units").alias("u"),
        F.percentile("latency_total_ms", 0.5).alias("p50"), F.percentile("latency_total_ms", 0.95).alias("p95"),
    ).collect()}
    bad = []
    for mo, x in truth["model_performance_overall"].items():
        r = rows[mo]
        if r["n"] != x["invocations"] or _eur_str(int(r["u"])) != x["cost_eur"] \
                or abs(r["p50"] - x["latency_p50_ms"]) > 0.5 or abs(r["p95"] - x["latency_p95_ms"]) > 0.5:
            bad.append(mo)
    _check(checks, "KPI-2", "model invocations, cost, p50/p95", not bad, str(bad))

    # ---- every priced ---------------------------------------------------------------------------------
    unpriced = gold["fact_inference"].filter("price_in_centi IS NULL").count()
    _check(checks, "FR-06", "every event priced with the tariff of its event date", unpriced == 0, f"{unpriced} unpriced")
    return checks


def assert_all(checks: list[dict]) -> None:
    failed = [c for c in checks if not c["ok"]]
    if failed:
        raise AssertionError("reconciliation failed: " + "; ".join(f"{c['id']} {c['name']}: {c['detail']}" for c in failed))
