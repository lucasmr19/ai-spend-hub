# KPI Definitions (single source of truth)

All KPIs are computed from the canonical Silver events (valid, deduplicated, v1/v2 normalised).
A KPI is **published only together with its data-quality status** (KPI-5).

## Conventions

- **Event date** = UTC date of `event_ts`. Month = calendar month of the event date.
- **Billable request** = `status = 'ok'` **and** `cache_status <> 'hit'`. Failed requests are not billed.
- **Cache hit** = served by the gateway; provider cost is 0.
- **Price** = tariff valid on the event date (half-open interval `[valid_from, valid_to)`).
- **Event cost** (EUR) = `tokens_in × price_in / 1e6 + tokens_out × price_out / 1e6`, computed in exact
  decimal arithmetic (8 decimals), never floating point.
- v1 events: `request_type` = app default, `cache_status` = `hit`/`miss`, `ttft_ms` = null ([migration](../contracts/migrations/v1_to_v2.json)).

## KPI-1 Spend by department (Q1)

| | |
|---|---|
| Definition | `SUM(event cost)` over billable requests, per department and month |
| Related | Budget utilisation = cost / approved monthly budget; driver breakdown by app and model |
| Grain | department × month (drill-down to app × model × day × event) |

## KPI-2 Model efficiency (Q2)

| | |
|---|---|
| Invocation | Billable request (a real model call) |
| Metrics | Cost per 1,000 invocations; latency p50 / p95 (exact percentile, linear interpolation); quality score from the evaluation table by `request_type` |
| Grain | model × request_type × month |
| Reading | Present the three axes together and mark non-dominated models per request type; no single composite score |

## KPI-3 Cache savings (Q3)

| | |
|---|---|
| Definition | `SUM(counterfactual cost)` over cache hits, where counterfactual cost = cost the event would have had at the tariff of its date |
| Related | Cache hit rate = hits / requests |
| Grain | app × month |

## KPI-4 Availability vs SLO (Q4)

| | |
|---|---|
| Definition | `100 × (1 − failed / total)` per app and month, failed = `status IN ('error','timeout')`; cache hits count as successful |
| Breach | availability < `target_availability_pct` of the app |
| Grain | app × month |

## KPI-5 Data reliability (Q5)

| Metric | Definition |
|---|---|
| Valid rate | valid records / records received |
| Duplicate rate | duplicate copies dropped / records received |
| Late rate | events arriving ≥ 1 day after the event date / unique valid events; very late = > 7 days |
| Freshness | latest landing date vs today |
| Quarantine | records per reason code ([codes](../contracts/validate.py)) |

**Publication gate (proposed thresholds, to be agreed with the client):** valid rate ≥ 97 %, zero
duplicate `event_id` in Silver, no schema-version outside {1, 2}, freshness ≤ 1 day. If the gate fails,
KPI tables keep their previous version and an alert is raised.
