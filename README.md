# AI Spend & Reliability Hub

Cost, model-efficiency and reliability analytics for a generative-AI gateway, built as a consultancy-style
data-engineering case study on **Databricks (Delta, Spark, Unity Catalog)** and **Snowflake (+ dbt)**.

> **Fictional case study.** *Northbeam Advisory* (consultancy) and *Halcyon Retail Group* (client) are
> invented. All data is synthetic. No real company commissioned or uses this solution.
> Platform cost: **€0** (free tiers only, no payment method — see [ADR-0001](docs/adr/0001-zero-cost-policy.md)).

## The problem

Six departments use 12 AI applications on four models with different spend limits. Management asks:

1. Which department consumes the most budget, and why?
2. Which model balances cost, latency and quality best?
3. How much spend does the cache avoid?
4. Which applications breach their availability objective?
5. Which data is reliable enough to report and to bill internally?

The answers must be traceable, consistently defined and quality-controlled — see
[requirements](docs/requirements.md) and [KPI definitions](docs/kpi-definitions.md).

## Architecture

```
 generator (Python)          Databricks (Free Edition)                       Snowflake (30-day trial)
 JSONL v1/v2 events  ──►  Landing ► Bronze ► Silver ► Gold (Delta)  ──►  Parquet via internal stage ► dbt marts ► BI
 + dimensions             Auto Loader · expectations · quarantine           RBAC · masking · resource monitor
                          dedupe · watermark · Unity Catalog
```

Why this split: [ADR-0002](docs/adr/0002-lakehouse-and-warehouse-split.md).

## What exists today (Weeks 1–2)

| Deliverable | Where | State |
|---|---|---|
| Requirements, acceptance criteria, KPI definitions | `docs/` | done |
| €0 policy with verified free-tier limits | `docs/adr/0001` | done |
| Data contracts v1 / v2, migration map, compatibility checker | `contracts/` | done |
| Synthetic generator with failure scenarios + exact ground truth | `generator/` | done |
| Reference pipeline (executable spec of Silver/Gold semantics) | `reference/` | done, reproduces ground truth exactly |
| Tests and CI | `tests/`, `.github/workflows/` | done |
| Spark logic: validation, dedupe, watermark, pricing, Gold SQL, reconciliation, gate, run log | `databricks/src/ai_spend/` | done — reconciles `ground_truth.json` exactly on a local Spark session (8/8 pre-backfill, 15/15 complete) |
| Databricks notebooks (Auto Loader → Silver `MERGE` → Gold → reconcile → backfill), job/bundle, runbook | `databricks/`, `docs/databricks-runbook.md` | written; **not yet run on a real Free Edition workspace** |
| Snowflake DDL, roles, dbt | `snowflake/`, `dbt/` | week 3 |
| Dashboard, runbook, cost model, case-study page | `docs/` | week 4 |

## Quick start (local, no accounts needed)

```bash
pip install -e ".[dev]"
make test            # contracts, generator, reference pipeline (+ Spark logic if pyspark is installed)
make demo            # generate ~370k events (≈120 MB, ~10 s) and run the reference pipeline
pip install -e ".[spark]"   # optional: local Spark + Delta for the Spark tests (needs Java 17+)
```

Presets: `small` (800 events/weekday, CI), `default` (5,000), `large` (25,000). Add `--gzip` for `.jsonl.gz` (19 MB for `default`) and `--layout flat` for a single upload folder.

Running it on Databricks Free Edition: [`docs/databricks-runbook.md`](docs/databricks-runbook.md).

## The dataset

3 months (2026-07-01 → 2026-09-30), deterministic (`--seed`). `landing/.../dt=` is the **arrival** date.
Deliberate scenarios, each with a known answer in `ground_truth.json`:

| Scenario | Result with the default preset |
|---|---|
| Duplicates (1.5 %), late (3 %) and very late (0.2 %) events | 5,573 duplicate copies · 13,975 late · 800 beyond the 7-day watermark |
| 7 kinds of corrupt records (reason codes E001–E103) | 2,889 records to quarantine |
| Schema v1 → v2 cutover app by app (2 legacy apps never migrate) | 242,780 v1 lines and 136,709 v2 lines coexist |
| Price cut on `aria-m` mid-period | requires price history |
| Runaway agent loop in Marketing (Sept) | Marketing €112 vs €44 budget — the only overrun |
| Upstream incident in Legal (Aug) | `contract-review` below its 99.5 % SLO — the only breach |

`reference/pipeline.py` reads only the landed files and dimension CSVs and reproduces cost per department /
model, cache savings, availability counts and every data-quality counter **exactly** (a test asserts it).
The Spark implementation (`databricks/src/ai_spend`) is tested to produce the same numbers, and a per-line test checks that its reason code equals the Python oracle's for all 60 k lines of the small preset.

## Repository map

```
docs/         requirements, KPI definitions, ADRs
contracts/    JSON Schemas v1/v2, migrations, validator (reason codes), compatibility checker
generator/    synthetic events, dimensions, scenarios, ground truth
reference/    pure-Python reference pipeline
databricks/   src/ai_spend (portable PySpark + SQL), notebooks 00–05, job/bundle definitions
tests/        pytest suite
```

Synthetic data only. Event volumes are scaled down; ratios and behaviours (not absolute euros) are what the case study demonstrates.
