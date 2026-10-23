"""Serving layer: contract, loaders, dbt marts vs ground truth (DuckDB always, PostgreSQL if SERVING_TEST_DSN is set)."""
import copy
import hashlib
import json
import os
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

import pytest

pytest.importorskip("pyspark")
pytest.importorskip("duckdb")
pytest.importorskip("psycopg")
pytest.importorskip("dbt.cli.main")

import duckdb  # noqa: E402
import psycopg  # noqa: E402
from ai_spend import export as ex  # noqa: E402
from ai_spend import local_run  # noqa: E402
from psycopg.conninfo import conninfo_to_dict  # noqa: E402

from generator.generate import run  # noqa: E402
from serving import bootstrap  # noqa: E402
from serving.contract import ContractError, check_manifest, load_contract  # noqa: E402
from serving.load_duckdb import load_export_duckdb  # noqa: E402
from serving.loader import load_export  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SALT = "test-salt"
PG_DSN = os.environ.get("SERVING_TEST_DSN")


@pytest.fixture(scope="module")
def export(tmp_path_factory):
    out = tmp_path_factory.mktemp("synthetic")
    truth = run(out, events_per_day=800, seed=42, preset="small")
    spark = local_run.local_session("serving-tests")
    try:
        built = local_run.build_all(spark, str(out))
        exp = tmp_path_factory.mktemp("export")
        manifest = ex.export_tables({**built["gold"], **ex.dimension_frames(built["dims"])}, str(exp))
    finally:
        spark.stop()
    return exp, manifest, truth


def _dbt(tmp_path, target, extra_env):
    profiles = tmp_path / "profiles"
    profiles.mkdir(exist_ok=True)
    (profiles / "profiles.yml").write_text((ROOT / "dbt" / "profiles.yml.example").read_text())
    env = {**os.environ, "DBT_PROFILES_DIR": str(profiles), "PII_SALT": SALT, "DBT_TARGET": target,
           "DBT_TARGET_PATH": str(tmp_path / "target"), "DBT_LOG_PATH": str(tmp_path / "logs"), **extra_env}
    r = subprocess.run([sys.executable, "-m", "dbt.cli.main", "build", "--project-dir", str(ROOT / "dbt")],
                       env=env, capture_output=True, text=True)
    assert r.returncode == 0, r.stdout[-3000:] + r.stderr[-1000:]


def _check_marts(q, truth):
    """q(sql) -> list of tuples. Every number must equal ground_truth.json."""
    rows = q("select department_id, month, cost_eur, budget_eur, over_budget from marts.mart_cost_by_department_month")
    assert len(rows) == sum(len(m) for m in truth["cost_by_department_month_eur"].values())
    for dept, month, cost, budget, over in rows:
        assert Decimal(str(cost)) == Decimal(truth["cost_by_department_month_eur"][dept][month])
        st = truth["budget_status_by_department_month"][dept][month]
        assert bool(over) == st["over_budget"] and Decimal(str(budget)) == Decimal(st["budget_eur"])
    for app, month, avail, breached in q("select app_id, month, availability_pct, breached from marts.mart_slo_compliance"):
        t = truth["slo_by_app_month"][app][month]
        assert bool(breached) == t["breached"] and abs(float(avail) - t["availability_pct"]) < 1e-6
    cache_sql = "select month, sum(cache_savings_eur), sum(cache_hits) from marts.mart_cache_savings group by 1"
    for month, savings, hits in q(cache_sql):
        assert Decimal(str(savings)) == Decimal(truth["cache_by_month"][month]["savings_eur"])
        assert int(hits) == truth["cache_by_month"][month]["hits"]
    by_model = dict(q("select model_id, sum(cost_units) from staging.stg_gold_model_performance group by 1"))
    for model, t in truth["model_performance_overall"].items():
        assert Decimal(str(by_model[model])) / Decimal(10) ** 8 == Decimal(t["cost_eur"])
    # masking: no clear user_id in dbt output, hash is salted sha256
    cols_sql = ("select column_name from information_schema.columns "
                "where table_schema = 'marts' and table_name = 'fct_inference'")
    cols = {c for (c,) in q(cols_sql)}
    assert "user_id" not in cols and "user_hash" in cols
    assert q("select count(*) from marts.mart_data_reliability")[0][0] > 0
    assert all(len(h) == 64 for (h,) in q("select user_hash from marts.fct_inference limit 50"))


def test_manifest_matches_contract_and_drift_is_refused(export):
    _, manifest, _ = export
    check_manifest(manifest, load_contract())
    for mutate in (lambda m: m["tables"]["gold_cost_daily"]["columns"].pop(),
                   lambda m: m["tables"]["gold_cost_daily"]["columns"][0].update(type="bigint"),
                   lambda m: m["tables"].pop("dim_apps")):
        bad = copy.deepcopy(manifest)
        mutate(bad)
        with pytest.raises(ContractError):
            check_manifest(bad, load_contract())


def test_duckdb_marts_reconcile_with_ground_truth(export, tmp_path):
    exp, _, truth = export
    db = tmp_path / "ai.duckdb"
    load_export_duckdb(exp, db)
    _dbt(tmp_path, "duckdb", {"DUCKDB_PATH": str(db)})
    con = duckdb.connect(str(db), read_only=True)
    _check_marts(lambda s: con.execute(s).fetchall(), truth)
    user = con.execute("select user_id from raw.fact_inference limit 1").fetchone()[0]
    h = con.execute("select user_hash from marts.fct_inference f join raw.fact_inference r using (event_id) "
                    "where r.user_id = ? limit 1", [user]).fetchone()[0]
    assert h == hashlib.sha256((SALT + user).encode()).hexdigest()


pg = pytest.mark.skipif(not PG_DSN, reason="set SERVING_TEST_DSN (admin DSN of a scratch PostgreSQL database)")


def _as(role, dbname_dsn):
    d = conninfo_to_dict(dbname_dsn)
    d.update(user=role, password="local-dev-only")
    return psycopg.connect(**d)


@pg
def test_postgres_marts_rbac_and_atomic_load(export, tmp_path):
    exp, manifest, truth = export
    load_export(exp, PG_DSN)
    bootstrap.bootstrap_roles(PG_DSN, dev=True)
    d = conninfo_to_dict(PG_DSN)
    _dbt(tmp_path, "postgres", {"PGHOST": d.get("host", "localhost"), "PGPORT": str(d.get("port", 5432)), "PGUSER": "ai_dbt",
                                "PGPASSWORD": "local-dev-only", "PGDATABASE": d["dbname"]})
    bootstrap.bootstrap_views(PG_DSN)

    with psycopg.connect(PG_DSN) as admin:
        _check_marts(lambda s: admin.execute(s).fetchall(), truth)

    def scalar(role, s):
        with _as(role, PG_DSN) as c:
            return c.execute(s).fetchall()

    # analysts and finance never see raw data or mart tables, only serving views
    for role in ("ai_analyst", "ai_finops"):
        for forbidden in ("select 1 from raw.fact_inference limit 1", "select 1 from marts.fct_inference limit 1",
                          "select user_id from serving.inference_with_user limit 1"):
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                scalar(role, forbidden)
    analyst_cols = _as("ai_analyst", PG_DSN).execute("select * from serving.inference limit 1").description
    assert "user_id" not in {c.name for c in analyst_cols}
    # steward sees the clear user_id, and only the steward
    uid = scalar("ai_steward", "select user_id from serving.inference_with_user limit 1")[0][0]
    assert uid.startswith("u-")
    # department head: own department only
    legal = {r[0] for r in scalar("ai_dept_legal", "select distinct department_id from serving.cost_by_department_month")}  # noqa: E501
    assert legal == {"legal"}
    assert {r[0] for r in scalar("ai_dept_legal", "select distinct department_id from serving.inference")} == {"legal"}
    with pytest.raises(psycopg.errors.InsufficientPrivilege):
        scalar("ai_dept_legal", "select * from serving.dept_access")
    finops_depts = {r[0] for r in scalar("ai_finops", "select distinct department_id from serving.chargeback")}
    assert len(finops_depts) == len(truth["budgets_eur_per_month"])
    # resource guard: the analyst's statement_timeout is enforced
    with _as("ai_analyst", PG_DSN) as c:
        c.execute("set statement_timeout = '200ms'")
        with pytest.raises(psycopg.errors.QueryCanceled):
            c.execute("select count(*) from generate_series(1, 10000000000)")

    # atomic load: a corrupted export is refused and the previous data stays intact
    before = psycopg.connect(PG_DSN).execute("select sum(cost_units) from raw.fact_inference").fetchone()[0]
    broken = tmp_path / "broken"
    broken.mkdir()
    for f in Path(exp).iterdir():
        (broken / f.name).write_bytes(f.read_bytes())
    m = json.loads((broken / "manifest.json").read_text())
    m["tables"]["gold_cost_daily"]["sums"]["cost_units"] = "1"
    (broken / "manifest.json").write_text(json.dumps(m))
    with pytest.raises(Exception, match="control sum"):
        load_export(broken, PG_DSN)
    assert psycopg.connect(PG_DSN).execute("select sum(cost_units) from raw.fact_inference").fetchone()[0] == before
