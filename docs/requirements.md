# Requirements Document — AI Spend & Reliability Hub

> **Fictional case study.** Northbeam Advisory (consultancy) and Halcyon Retail Group (client) are invented.
> All data is synthetic. No real company commissioned or uses this solution.

| | |
|---|---|
| Version | 0.1 (Week 1) |
| Status | Draft for sign-off |
| Author | Data engineering workstream |
| Related | [KPI definitions](kpi-definitions.md) · [ADR-0001 €0 policy](adr/0001-zero-cost-policy.md) · [ADR-0002 platform split](adr/0002-lakehouse-and-warehouse-split.md) · [contracts](../contracts/README.md) |

## 1. Context

Northbeam Advisory runs an AI gateway that serves 12 generative-AI applications for 6 departments of
Halcyon Retail Group. Each department uses different models and has its own monthly spend limit. Today
usage logs sit in separate files, prices change over time, and nobody can answer basic questions with
numbers that finance would accept for **internal chargeback**.

## 2. Business questions

| # | Question | Primary user | KPI |
|---|---|---|---|
| Q1 | Which department consumes the most budget, and why? | CFO, department heads | KPI-1 |
| Q2 | Which model gives the best balance of cost, latency and quality? | Head of AI platform | KPI-2 |
| Q3 | How much spend does the cache avoid? | FinOps analyst | KPI-3 |
| Q4 | Which applications breach their availability objective? | Platform / SRE lead | KPI-4 |
| Q5 | Which data is reliable enough to report to management and to bill internally? | Data steward, Finance | KPI-5 |

The answers must be **traceable** (every number drills down to event ids), **consistent** (one definition
per KPI) and **controlled** (automated quality checks gate publication). A good-looking dashboard alone is
not acceptable.

## 3. Stakeholders and users

| Role | Needs | Access |
|---|---|---|
| CFO / COO | Monthly spend vs budget per department, trusted figures | Dashboard (aggregates only) |
| Department head | Own spend, top apps, drivers of change | Dashboard filtered to own department |
| FinOps analyst | Cost by app/model/day, cache savings, chargeback extract | Marts, no direct user identifiers |
| Platform / SRE lead | Availability vs SLO, error causes, incident impact | SLO marts |
| Data steward | Data-quality status, quarantine, lineage | DQ marts, quarantine tables |
| Data engineer (operator) | Reproducible deployment, runbook, alerts | Full pipeline access |

## 4. Scope

**In scope:** ingestion of gateway events (contract v1 and v2), validation, deduplication, late-data
handling, cost attribution with historical prices, SLO and cache KPIs, data-quality KPIs, role-based
serving layer, business dashboard, CI/CD, runbook.

**Out of scope:** prompt/response content, real-time (<1 min) streaming, model-quality evaluation
execution (a precomputed evaluation table is consumed), real LLM calls.

## 5. Functional requirements

| ID | Requirement |
|---|---|
| FR-01 | Ingest JSONL events incrementally from a landing area; reprocessing a file must not create duplicates. |
| FR-02 | Accept contract v1 and v2 simultaneously and normalise to one canonical schema. |
| FR-03 | Validate every record against the contract and business rules; invalid records go to quarantine with a reason code ([codes](../contracts/validate.py)). |
| FR-04 | Deduplicate on `event_id` (first arrival wins). |
| FR-05 | Handle late events up to a 7-day watermark automatically; later events go to a late-quarantine and are applied by a documented backfill. |
| FR-06 | Price each event with the tariff valid on the **event date** (price history table). |
| FR-07 | Produce KPI-1 … KPI-5 with the definitions in [kpi-definitions.md](kpi-definitions.md). |
| FR-08 | Publish KPI tables only when the data-quality gate passes; otherwise keep the previous version and raise an alert. |
| FR-09 | Serve marts to BI with role-based access; `user_id` masked for non-privileged roles. |
| FR-10 | Provide a chargeback view: monthly cost per department and app, reproducible from event ids. |

## 6. Non-functional requirements

| ID | Requirement |
|---|---|
| NFR-01 **Cost** | Total platform cost **€0**: free tiers only, no payment method on any account ([ADR-0001](adr/0001-zero-cost-policy.md)). |
| NFR-02 **Freshness** | Gold KPIs refreshed daily; data-quality dashboard shows freshness (max arrival lag). |
| NFR-03 **Idempotency** | Re-running any day/job yields identical row counts and checksums. |
| NFR-04 **Observability** | Each run records start/end, rows in/out/quarantined, duration, status; alert on gate failure or freshness breach. |
| NFR-05 **Security** | No secrets in the repository; least-privilege roles; no direct identifiers in the contract; masking on pseudonymous user key. |
| NFR-06 **Reproducibility** | Fresh clone → `make demo` produces the full local result; infrastructure and DDL are versioned code. |
| NFR-07 **Retention** | 13 months of event history (design target; demo data covers 3 months). |
| NFR-08 **Portability** | Transformations expressed as SQL/dbt models that run on DuckDB locally and on Snowflake. |

## 7. Acceptance criteria

Every criterion is checked against the synthetic dataset's `ground_truth.json`.

| ID | Criterion | Check |
|---|---|---|
| AC-01 | Monthly cost per department and per model equals ground truth. | Exact to €0.01 after the backfill of very-late events |
| AC-02 | No duplicate `event_id` in Silver; dropped duplicates equal `dq.duplicate_copies`. | Automated |
| AC-03 | Quarantine holds every invalid record with the correct reason code; counts per code equal ground truth. | Automated |
| AC-04 | v1 and v2 files processed by the same job; no failure at cutover; Silver has a single schema. | Automated |
| AC-05 | Late events ≤ 7 days land in the right event-date aggregate; later ones are quarantined and applied by backfill. | Automated |
| AC-06 | Availability per app-month equals ground truth (±0.001 pp); breaches flagged (expected: `contract-review`, 2026-08). | Automated |
| AC-07 | Cache savings equal ground truth to €0.01. | Automated |
| AC-08 | A deliberately corrupted batch makes the quality gate fail and KPI tables stay unchanged. | Failure demo |
| AC-09 | Re-running a day produces identical tables. | Automated |
| AC-10 | `user_id` is masked for the analyst role; no secrets in the repo (secret scan in CI). | Automated |
| AC-11 | CI green on every merge: lint, tests, contract compatibility, dbt build. | CI |
| AC-12 | No payment method registered on any platform; Snowflake resource monitor configured. | Checklist in runbook |

## 8. Known scenarios in the data (for the demo)

| Scenario | Where | Expected finding |
|---|---|---|
| Runaway agent loop | `campaign-writer` (Marketing), 2026-09-09 → 09-11 | Marketing over budget in September (Q1 "why") |
| Upstream incident | `contract-review` (Legal), 2026-08-27, 13:00–19:00 UTC | Availability SLO breached in August (Q4) |
| Price cut | `aria-m`, 2026-08-15 | Same tokens cost less after the cut — needs price history (FR-06) |
| Schema change v1 → v2 | From 2026-08-20, app by app; `hr-assistant` and `invoice-extractor` never migrate | Both versions live indefinitely (FR-02) |
| Collector outage | 2026-08-12, 09:00–15:00 UTC | Burst of 1-day-late events (FR-05) |
| Duplicates, late, very late, corrupt records | Whole period | DQ KPIs and quarantine (FR-03/04/05) |

## 9. Assumptions and risks

| Risk | Mitigation |
|---|---|
| Databricks Free Edition: serverless only, daily quota, may delete inactive accounts | Keep jobs small; commit all code; verify Lakeflow/Jobs/bundle support early in week 2 |
| Snowflake trial lasts 30 days and data is deleted afterwards | Start Snowflake work only when Databricks side is ready; everything as idempotent scripts; capture evidence (screenshots, query history, dbt docs) before expiry |
| Platforms can change free-tier terms | Re-verify limits at the start of each phase ([ADR-0001](adr/0001-zero-cost-policy.md)) |
| Free Edition is non-commercial | Used only for a personal portfolio project, never for client work |

## 10. Glossary

**Showback/chargeback** — reporting / invoicing internal consumption. **SLO** — service-level objective.
**Watermark** — maximum lateness accepted by the streaming/incremental job. **Quarantine** — table that
keeps rejected records and the reason. **Canonical schema** — the single Silver shape both contract
versions are mapped to.
