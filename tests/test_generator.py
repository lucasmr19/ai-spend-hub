import gzip
import json
from datetime import date
from decimal import Decimal

import pytest

from generator import dimensions as dm
from generator.generate import run
from reference.pipeline import run as run_reference


@pytest.fixture(scope="module")
def dataset(tmp_path_factory):
    out = tmp_path_factory.mktemp("synthetic")
    truth = run(out, events_per_day=800, seed=42, preset="small")
    return out, truth


@pytest.fixture(scope="module")
def reference(dataset):
    return run_reference(dataset[0])


def test_reference_pipeline_reproduces_ground_truth_dq(dataset, reference):
    _, truth = dataset
    r, t = reference["dq"], truth["dq"]
    assert r["lines_read"] == t["lines_emitted"]
    assert r["lines_by_schema_version"] == t["lines_by_schema_version"]
    assert r["unique_valid_events"] == t["unique_valid_events"]
    assert r["duplicate_copies"] == t["duplicate_copies"]
    assert r["invalid_by_code"] == t["invalid_by_code"]  # exact reason codes, not just totals


def test_reference_pipeline_reproduces_costs_exactly(dataset, reference):
    _, truth = dataset
    assert reference["cost_by_department_month_eur"] == truth["cost_by_department_month_eur"]
    assert reference["cost_by_model_month_eur"] == {
        m: v for m, v in truth["cost_by_model_month_eur"].items()
    }
    assert reference["cache_savings_by_month_eur"] == {
        m: x["savings_eur"] for m, x in truth["cache_by_month"].items()
    }


def test_reference_pipeline_reproduces_slo_counts(dataset, reference):
    _, truth = dataset
    for app, months in truth["slo_by_app_month"].items():
        for month, x in months.items():
            r = reference["slo_by_app_month"][app][month]
            assert (r["total"], r["failed"]) == (x["total"], x["failed"])


def test_all_scenarios_present(dataset):
    _, t = dataset
    dq = t["dq"]
    assert dq["duplicate_copies"] > 0
    assert dq["late_events_1_to_7d"] > 0 and dq["very_late_events_gt_7d"] > 0
    assert set(dq["invalid_by_code"]) == {"E001", "E002", "E003", "E004", "E101", "E102", "E103"}
    assert dq["lines_by_schema_version"]["v1"] > 0 and dq["lines_by_schema_version"]["v2"] > 0


def test_marketing_overruns_budget_only_in_september(dataset):
    _, t = dataset
    mk = t["budget_status_by_department_month"]["marketing"]
    assert [m for m, x in mk.items() if x["over_budget"]] == ["2026-09"]
    others = [
        (d, m) for d, ms in t["budget_status_by_department_month"].items() if d != "marketing"
        for m, x in ms.items() if x["over_budget"]
    ]
    assert others == []


def test_contract_review_breaches_slo_in_august_only(dataset):
    _, t = dataset
    breaches = [(a, m) for a, ms in t["slo_by_app_month"].items() for m, x in ms.items() if x["breached"]]
    assert ("contract-review", "2026-08") in breaches
    assert ("contract-review", "2026-07") not in breaches


def test_engineering_is_top_spender_and_cache_saves_money(dataset):
    _, t = dataset
    totals = {d: sum(Decimal(v) for v in ms.values()) for d, ms in t["cost_by_department_month_eur"].items()}
    assert max(totals, key=totals.get) == "engineering"
    assert all(Decimal(x["savings_eur"]) > 0 for x in t["cache_by_month"].values())


def test_schema_v2_cutover_dates_and_legacy_apps(dataset):
    out, _ = dataset
    first_v2: dict[str, str] = {}
    v2_apps_seen = set()
    for path in sorted((out / "landing" / "inference_events").glob("dt=*/*")):
        for line in path.read_text().splitlines():
            r = json.loads(line)
            if r.get("schema_version") == 2:
                v2_apps_seen.add(r["app_id"])
                first_v2.setdefault(r["app_id"], r["event_ts"][:10])
    for app in dm.APPS:
        if app.v2_from is None:
            assert app.app_id not in v2_apps_seen  # legacy emitters never migrate
        else:
            assert first_v2[app.app_id] >= app.v2_from.isoformat()


def test_price_cut_changes_cost_of_same_tokens():
    before, after = dm.price_on("aria-m", date(2026, 8, 14)), dm.price_on("aria-m", date(2026, 8, 15))
    assert before == (80, 320) and after == (60, 240)


def test_dimensions_and_budgets_written(dataset):
    out, _ = dataset
    for name in ["departments", "apps", "models", "model_prices", "slo_targets", "model_quality", "budgets"]:
        assert (out / "dimensions" / f"{name}.csv").exists()


def test_generation_is_deterministic_and_gzip_is_reproducible(tmp_path):
    from datetime import date as d

    kw = dict(events_per_day=300, seed=7, start=d(2026, 8, 10), end=d(2026, 8, 13))
    a, b = tmp_path / "a", tmp_path / "b"
    ta, tb = run(a, gzip_files=True, **kw), run(b, gzip_files=True, **kw)
    assert ta == tb
    ma, mb = json.loads((a / "manifest.json").read_text()), json.loads((b / "manifest.json").read_text())
    assert ma == mb
    f = next((a / "landing" / "inference_events").glob("dt=*/*.gz"))
    assert gzip.decompress(f.read_bytes()).count(b"\n") > 0
    other = run(tmp_path / "c", gzip_files=True, **{**kw, "seed": 8})
    assert other["dq"] != ta["dq"]
