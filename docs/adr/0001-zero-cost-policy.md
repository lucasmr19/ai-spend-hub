# ADR-0001 — €0 cost policy and free-tier guardrails

Status: accepted · Date: 2026-10-19

## Context

The project owner has no cloud budget. Every platform used must stay at €0, with no risk of a surprise bill.

## Facts verified on 2026-10-19 (official docs and vendor pages — re-verify at each phase)

| Platform | What the free offer is | Why it cannot bill | Constraints that shape the design |
|---|---|---|---|
| **Databricks Free Edition** | Permanent no-cost account, **serverless compute only**, fair-usage quota | Exceeding the quota **shuts compute down for the rest of the day (in extreme cases the month)**; data and settings are kept | No custom clusters or GPUs; non-commercial use only; no SLA; inactive accounts may be deleted; run only 1–2 workloads in parallel |
| **Snowflake trial** | 30 days and **$400 of credit, no billing information required** | Account is suspended when credits or days run out; deactivated if no billing info is added within 30 days after the trial | Data may be deleted after the trial; external network access reportedly limited to 10 credits/day until payment is added |
| **GitHub (public repo)** | Free Actions minutes for public repositories | n/a | Keep the repository public; no secrets in it |

Sources: Databricks "Free Edition limitations" documentation; Snowflake trial FAQ.

## Decision

1. **Never add a payment method** to Databricks, Snowflake, GitHub or any cloud. This is the single hard rule.
2. **Local-first.** The generator, the reference pipeline and (later) the dbt models run on a laptop with Python and DuckDB. The cloud platforms are used to demonstrate the platform-specific parts, not to develop.
3. **No cloud object storage, no paid LLM API, no managed services outside the two free offers.** Events are synthetic.
4. **File-based hand-off between platforms.** Databricks writes Gold tables as Parquet to a Unity Catalog volume; the files are downloaded and loaded into Snowflake through an **internal stage** (`PUT` + `COPY INTO`). No bucket, no egress, no direct network dependency between the two free accounts.
5. **Snowflake is time-boxed.** The account is created only when the Databricks side and the dbt models (tested on DuckDB) are finished. All Snowflake work is idempotent SQL/dbt kept in the repo, so a new trial can recreate everything. Before expiry: export screenshots, query history, dbt docs/lineage and the resource-monitor configuration as portfolio evidence.
6. **Defensive settings anyway:** XS warehouse, `AUTO_SUSPEND = 60`, a resource monitor with a low quota, no always-on features.
7. **Databricks runs stay small:** default dataset (~0.4 M events) fits comfortably; avoid parallel jobs.

## Consequences

- No fixed cost, no billing risk; the worst case is a paused account.
- Evidence of the Snowflake work must be captured during the trial — it will not be there afterwards.
- A production cost estimate (DBUs, credits, storage) is a documentation exercise for week 4 using the vendors' current price lists; it does not involve spending.
- Week 2 outcome: the pipeline is implemented as portable Spark logic + notebooks (Auto Loader, Delta `MERGE`, Jobs). The maintainer's Free Edition account exists; **still to confirm on it**: Auto Loader on a volume, `MERGE`, serverless jobs and bundle deployment. A Lakeflow Declarative Pipelines (expectations) variant is deferred until those are confirmed.
