# ADR-0002 — Databricks for processing, Snowflake for serving

Status: accepted · Date: 2026-10-19

## Context

The portfolio goal is hands-on evidence for both Databricks and Snowflake without using either superficially.
Consultancies commonly pair a lakehouse (heavy ingestion, semi-structured data, data quality) with a
warehouse (governed SQL serving to BI and finance).

## Decision

| Layer | Platform | Reason |
|---|---|---|
| Landing → Bronze → Silver → Gold | **Databricks** (Delta, Auto Loader, declarative pipelines with expectations, Unity Catalog) | Semi-structured JSON, schema evolution, quarantine and expectations are first-class |
| Marts, RBAC, masking, chargeback views, BI | **Snowflake** (+ dbt) | Governed SQL serving, role hierarchy, masking policies, resource monitors |
| Local development | Python + DuckDB | €0 and fast feedback (see ADR-0001) |

Responsibilities do not overlap: **Silver and Gold facts are produced in Databricks only**; Snowflake
never re-derives cost or availability, it serves and governs them. KPI logic therefore has one owner.

## Alternatives considered

- *Databricks only:* simpler, but no warehouse-side governance story.
- *Snowflake only:* weaker fit for file-based incremental ingestion with schema drift on the free tier.
- *Both end-to-end:* duplicated logic, shallow on both.

## Consequences

- Interview narrative: why each tool sits where it does, and what it costs to keep them consistent
  (a contract between Gold and the Snowflake load, checked by row counts and checksums).
- dbt models are written in portable SQL and tested on DuckDB, then pointed at Snowflake.
