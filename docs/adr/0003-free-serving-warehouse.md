# ADR-0003 — A free, Snowflake-like serving layer: PostgreSQL + dbt (DuckDB for dev/CI)

Status: accepted · Date: 2026-10-22 · Supersedes the Snowflake part of ADR-0002

## Context
The case study needs a governed SQL serving layer after the Databricks lakehouse (Gold). The original plan
used a Snowflake trial. Constraints: **cost must stay at €0** (ADR-0001), no payment method anywhere, and the work
must remain reproducible after a trial would have expired. Snowflake's trial is time-boxed (30 days, credit-limited,
data deleted afterwards), so the evidence would disappear. Open-source candidates considered: ClickHouse,
TimescaleDB, Cube, Apache Druid, Databend, PostgreSQL, DuckDB.

## What each candidate offers for *this* workload (checked in vendor docs on 2026-10-22; re-verify before quoting)
| Need | Snowflake | PostgreSQL 16 | ClickHouse | Databend |
|---|---|---|---|---|
| Roles + grants | yes | yes | yes | yes |
| Row-level filtering | row access policies | RLS or secure views | row policies | row access policy |
| Column masking | masking policies | masked views (any role-aware view) | no documented dynamic masking | masking policy is **Enterprise-only** |
| Resource limits | resource monitors | `statement_timeout`, `CONNECTION LIMIT` | quotas / settings profiles | limited |
| Runs in the sandbox / CI for free | no | **yes** (service container) | not installable here | not installable here |
| dbt adapter | yes | yes (dbt-postgres) | community | community |
Cube is a semantic/metrics layer and Druid a real-time OLAP store: neither is a warehouse with governance, so they
solve a different problem. Volume here is ~380 k events; analytical columnar engines bring no benefit at this size.

## Decision
- **PostgreSQL 16** is the governed serving warehouse (roles, grants, role-aware views, resource limits).
- **DuckDB** runs the same dbt models for development and CI (no server).
- **dbt models are portable SQL** (only the masking macro dispatches per engine), so moving to Snowflake/BigQuery is a
  profile + a handful of macros, not a rewrite. Snowflake stays a documented migration target, not a dependency.
- The Gold → serving hand-off is a **versioned contract** (`serving/contract/gold_export.v1.json`) plus a manifest with
  row counts and control sums; the loader refuses a mismatching export and loads in one transaction.

## Consequences
- (+) €0 forever, reproducible by anyone, verified in CI against `ground_truth.json` on both engines.
- (+) Real defects were found by those tests (float rounding in a mart, DROP breaking dependent views, wrong identity in a
  SECURITY DEFINER function) — see `progress.md`.
- (−) Postgres is row-oriented and single-node: no elastic compute, no time travel, no zero-copy clone. Not a claim of
  Snowflake experience; the CV line must say "Snowflake-equivalent governance patterns on PostgreSQL, portable dbt".
- (−) Row filtering uses secure views instead of RLS because dbt recreates the tables (policies would be dropped).
  Views are owned by the admin role and use `session_user`; a user able to `SET ROLE` to another member is out of scope.
- (−) Masking is salted SHA-256 pseudonymisation, not format-preserving or reversible; the salt lives in the environment
  (`PII_SALT`), never in git. Rotating the salt changes every hash.
