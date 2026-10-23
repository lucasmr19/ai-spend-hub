# progress.md — AI Spend & Reliability Hub

Last update: 2026-10-21 · Package: `ai-spend-hub-week2.zip` (Week 2 executed on your Databricks Free Edition; Week 3 not started)

## Status

| Week | Scope                                                                                                              | State                                                                                                                               |
| ---- | ------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- |
| 1    | Requirements, KPI definitions, €0 policy, contracts v1/v2, generator + ground truth, reference pipeline, tests, CI | **done**                                                                                                                            |
| 2    | Databricks: Spark logic, notebooks, gate, run log, runbook                                                         | **done** — all notebooks ran on Free Edition (reported by you); reconciliation results still to be confirmed with the queries below |
| 3    | Snowflake load, dbt marts + tests (DuckDB first), RBAC, masking, resource monitor                                  | not started                                                                                                                         |
| 4    | Dashboard, runbook (ops), cost model, portfolio case-study page                                                    | not started                                                                                                                         |

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

## Findings from the first real run (2026-10-21)

- Notebooks 00 and 01 ran on Free Edition (setup + Auto Loader OK).
- `02_silver` failed: `.cache()` is not supported on serverless (`NOT_SUPPORTED_WITH_SERVERLESS: PERSIST TABLE`). Fixed: the classification is materialised in a staging Delta table `_stg_classified`. No other `cache/persist/rdd/sparkContext` use in `databricks/`. Lesson: local Spark accepts APIs that serverless (Spark Connect) rejects, so each notebook needs a real first run.

## Confirmed on Databricks Free Edition (user-reported, 2026-10-21)

- Notebooks 00 → 05 and both reconcile phases ran without further errors after the `cache()` fix. This confirms Auto Loader on a volume, Delta `MERGE`, serverless execution and the `../src` bootstrap.
- To record the evidence, run in the SQL editor: `ops_reconciliation` (expect 8 OK in `pre_backfill`, 15 OK in `complete`), `ops_run_log`, `gold_budget_vs_actual`, `gold_slo_monthly`.

## Still not verified

1. Job / bundle deployment (optional path).
2. Failure demo from runbook section 5 (gate blocking a poisoned batch) on the real workspace.

## Superseded checklist (kept for history)

1. ~~Auto Loader on a volume~~ — confirmed working (notebook 01 ran).
2. Delta `MERGE … WHEN NOT MATCHED INSERT *` — the sandbox cannot download Delta jars, so MERGE was not tested locally.
3. Notebook path bootstrap (`../src`), widget defaults (catalog name `workspace`?), `display`, serverless quota with ~380 k lines.
4. Bundle/job deployment on Free Edition (optional path).

## Next steps

You: confirm reconciliation (queries), run the failure demo (runbook section 5), capture screenshots (section 8). **Do not create the Snowflake account yet** (30-day clock).
Me (Week 3, no account needed): `06_export_gold` notebook (Gold → Parquet in the volume), dbt project tested on DuckDB with the local Gold, Snowflake DDL/roles/masking scripts. Snowflake trial is created only when those are ready.

## Open items

- Lakeflow Declarative Pipelines (expectations) variant — deferred until step 1–2 above are confirmed.
- BI tool choice (Power BI vs Streamlit in Snowflake) — Week 4.
- Gate thresholds are proposals to agree with the client.
- CI on GitHub: first run failed on `pip install` (setuptools auto-discovery of several top-level folders). Fixed with `[tool.setuptools] py-modules = []`; actions bumped (checkout v5, setup-python v6, setup-java v5) and runner pinned to `ubuntu-24.04`. Verified on a clean copy (install, ruff, 27 tests) — the next GitHub run is still to be confirmed.
- Production cost estimate in Week 4 from current vendor price lists.

## How to run locally

```bash
pip install -e ".[dev,spark]"
make test
make demo
```
