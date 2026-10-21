# progress.md — AI Spend & Reliability Hub

Last update: 2026-10-20 · Package: `ai-spend-hub-week2.zip` (provisional: Week 2 written and tested locally, not yet run on Databricks)

## Status

| Week | Scope | State |
|---|---|---|
| 1 | Requirements, KPI definitions, €0 policy, contracts v1/v2, generator + ground truth, reference pipeline, tests, CI | **done** |
| 2 | Databricks: Spark logic, notebooks, gate, run log, runbook | **written + tested locally; waiting for first run on your Free Edition workspace** |
| 3 | Snowflake load, dbt marts + tests (DuckDB first), RBAC, masking, resource monitor | not started |
| 4 | Dashboard, runbook (ops), cost model, portfolio case-study page | not started |

## What was done in Week 2

- `databricks/src/ai_spend/transform.py`: validation (same reason codes E000–E103 as `contracts/validate.py`), dedupe (first arrival wins, **before** the watermark rule), 7-day watermark → late quarantine, v1/v2 → canonical Silver.
- `gold.py`: Spark SQL for `fact_inference` + 6 Gold tables (cost daily, budget vs actual, SLO monthly, model performance, DQ daily, quarantine summary); money as exact integer units.
- `gate.py` (publication gate, AC-08), `ops.py` (run log), `reconcile.py` (AC-01…AC-07 vs `ground_truth.json`).
- `databricks/notebooks/00–05` (setup, Auto Loader bronze, Silver `MERGE`, gated Gold, reconcile, backfill), `databricks.yml` + job definitions, `docs/databricks-runbook.md`.
- Generator: `--layout flat` (single upload folder) and `--gzip`; default dataset is 19 MB gzipped.
- Tests: 27 green (Spark tests need `pip install -e ".[spark]"` and Java 17+); ruff clean; CI updated to run them.

## Verified (local Spark 4.0.1, default preset, seed 42, gzip + flat layout)

- Pre-backfill reconciliation 8/8; after backfill 15/15 — costs per department/model, budget overruns, SLO breaches, cache savings, p50/p95, DQ counters all equal `ground_truth.json`.
- Spark reason code == Python oracle reason code for every one of 60,772 lines (small preset).
- Gate passes on clean data and blocks a poisoned batch.

## NOT verified yet (needs your Databricks account)

1. Auto Loader (`cloudFiles`, text, `availableNow`) on a Unity Catalog volume in Free Edition.
2. Delta `MERGE … WHEN NOT MATCHED INSERT *` — the sandbox cannot download Delta jars, so MERGE was not tested locally.
3. Notebook path bootstrap (`../src`), widget defaults (catalog name `workspace`?), `display`, serverless quota with ~380 k lines.
4. Bundle/job deployment on Free Edition (optional path).

## Next step for you

Follow `docs/databricks-runbook.md` sections 2–4 (upload data, run notebooks 00→05) and send me the exact error text of anything that fails, plus your catalog name. I will fix paths/settings; the logic is already reconciled.

## Open items

- Lakeflow Declarative Pipelines (expectations) variant — deferred until step 1–2 above are confirmed.
- BI tool choice (Power BI vs Streamlit in Snowflake) — Week 4.
- Gate thresholds are proposals to agree with the client.
- Repo not pushed to GitHub yet (CI has never run remotely).
- Production cost estimate in Week 4 from current vendor price lists.

## How to run locally

```bash
pip install -e ".[dev,spark]"
make test
make demo
```
