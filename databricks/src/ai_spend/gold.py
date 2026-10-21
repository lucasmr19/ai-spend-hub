"""Silver -> Gold in Spark SQL.

All money is carried as integer ``*_units`` (1 unit = 1e-8 EUR = tokens x centi-EUR per Mtok) and
converted to ``DECIMAL(20,8)`` only at the edge, so sums are exact and reproducible.

Views expected by the SQL (register them with ``register_views``):
    silver_events, classified_lines, dim_model_prices, dim_budgets, dim_slo_targets, dim_model_quality
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

EUR = "CAST(CAST({units} AS DECIMAL(28,0)) / 100000000 AS DECIMAL(20,8))"


def _eur(units_expr: str) -> str:
    return EUR.format(units=units_expr)


FACT_SQL = """
SELECT s.*,
       p.price_in_centi, p.price_out_centi,
       s.tokens_in * p.price_in_centi + s.tokens_out * p.price_out_centi AS cost_units,
       (s.status = 'ok' AND s.cache_status <> 'hit') AS is_billable,
       (s.status = 'ok' AND s.cache_status = 'hit')  AS is_cache_hit
FROM silver_events s
LEFT JOIN dim_model_prices p
  ON s.model_id = p.model_id
 AND s.event_date >= p.valid_from
 AND (p.valid_to IS NULL OR s.event_date < p.valid_to)
"""

COST_DAILY_SQL = f"""
SELECT event_date, department_id, app_id, model_id, request_type,
       COUNT(*)                                             AS requests,
       SUM(CASE WHEN is_billable   THEN 1 ELSE 0 END)       AS billable_requests,
       SUM(CASE WHEN is_cache_hit  THEN 1 ELSE 0 END)       AS cache_hits,
       SUM(CASE WHEN status <> 'ok' THEN 1 ELSE 0 END)      AS failed_requests,
       SUM(tokens_in)                                       AS tokens_in,
       SUM(tokens_out)                                      AS tokens_out,
       SUM(CASE WHEN is_billable  THEN cost_units ELSE 0 END) AS cost_units,
       SUM(CASE WHEN is_cache_hit THEN cost_units ELSE 0 END) AS cache_savings_units,
       {_eur("SUM(CASE WHEN is_billable  THEN cost_units ELSE 0 END)")} AS cost_eur,
       {_eur("SUM(CASE WHEN is_cache_hit THEN cost_units ELSE 0 END)")} AS cache_savings_eur
FROM fact_inference
GROUP BY event_date, department_id, app_id, model_id, request_type
"""

BUDGET_SQL = f"""
WITH m AS (
  SELECT department_id, date_format(event_date, 'yyyy-MM') AS month, SUM(cost_units) AS cost_units
  FROM gold_cost_daily GROUP BY department_id, date_format(event_date, 'yyyy-MM')
)
SELECT m.department_id, m.month, m.cost_units,
       {_eur("m.cost_units")} AS cost_eur,
       b.budget_eur,
       ROUND(100 * {_eur("m.cost_units")} / b.budget_eur, 1) AS utilisation_pct,
       {_eur("m.cost_units")} > b.budget_eur AS over_budget
FROM m LEFT JOIN dim_budgets b ON m.department_id = b.department_id AND m.month = b.month
"""

SLO_SQL = """
WITH a AS (
  SELECT app_id, date_format(event_date, 'yyyy-MM') AS month,
         COUNT(*) AS total, SUM(CASE WHEN status <> 'ok' THEN 1 ELSE 0 END) AS failed
  FROM fact_inference GROUP BY app_id, date_format(event_date, 'yyyy-MM')
)
SELECT a.app_id, a.month, a.total, a.failed,
       ROUND(100.0 * (1 - a.failed / a.total), 4) AS availability_pct,
       t.target_availability_pct AS target_pct,
       100.0 * (1 - a.failed / a.total) < t.target_availability_pct AS breached
FROM a LEFT JOIN dim_slo_targets t ON a.app_id = t.app_id
"""

MODEL_PERF_SQL = f"""
WITH i AS (
  SELECT model_id, request_type, date_format(event_date, 'yyyy-MM') AS month,
         COUNT(*) AS invocations, SUM(cost_units) AS cost_units,
         percentile(latency_total_ms, 0.5)  AS latency_p50_ms,
         percentile(latency_total_ms, 0.95) AS latency_p95_ms
  FROM fact_inference WHERE is_billable
  GROUP BY model_id, request_type, date_format(event_date, 'yyyy-MM')
)
SELECT i.model_id, i.request_type, i.month, i.invocations, i.cost_units,
       {_eur("i.cost_units")} AS cost_eur,
       ROUND({_eur("i.cost_units")} / i.invocations * 1000, 6) AS cost_per_1k_invocations_eur,
       i.latency_p50_ms, i.latency_p95_ms, q.quality_score
FROM i LEFT JOIN dim_model_quality q ON i.model_id = q.model_id AND i.request_type = q.request_type
"""

DQ_DAILY_SQL = """
SELECT arrival_date,
       COUNT(*)                                                        AS lines_received,
       SUM(CASE WHEN disposition = 'silver'          THEN 1 ELSE 0 END) AS accepted,
       SUM(CASE WHEN disposition = 'duplicate'       THEN 1 ELSE 0 END) AS duplicates_dropped,
       SUM(CASE WHEN disposition = 'late_quarantine' THEN 1 ELSE 0 END) AS late_quarantined,
       SUM(CASE WHEN disposition = 'quarantine'      THEN 1 ELSE 0 END) AS invalid_quarantined,
       ROUND(100.0 * (1 - SUM(CASE WHEN disposition = 'quarantine' THEN 1 ELSE 0 END) / COUNT(*)), 3)
                                                                       AS valid_rate_pct,
       SUM(CASE WHEN disposition IN ('silver','late_quarantine') AND lag_days >= 1 THEN 1 ELSE 0 END)
                                                                       AS late_arrivals
FROM classified_lines GROUP BY arrival_date
"""

QUARANTINE_SUMMARY_SQL = """
SELECT reason AS reason_code, COUNT(*) AS records, MIN(arrival_date) AS first_arrival, MAX(arrival_date) AS last_arrival
FROM classified_lines WHERE disposition = 'quarantine' GROUP BY reason
"""


def register_views(spark: SparkSession, *, silver: DataFrame, classified: DataFrame, dims: dict[str, DataFrame]) -> None:
    silver.createOrReplaceTempView("silver_events")
    classified.createOrReplaceTempView("classified_lines")
    dims["model_prices"].createOrReplaceTempView("dim_model_prices")
    dims["budgets"].createOrReplaceTempView("dim_budgets")
    dims["slo_targets"].createOrReplaceTempView("dim_slo_targets")
    dims["model_quality"].createOrReplaceTempView("dim_model_quality")


def build_gold(spark: SparkSession) -> dict[str, DataFrame]:
    """Build every Gold table from the registered views. Returns name -> DataFrame."""
    fact = spark.sql(FACT_SQL)
    fact.createOrReplaceTempView("fact_inference")
    cost_daily = spark.sql(COST_DAILY_SQL)
    cost_daily.createOrReplaceTempView("gold_cost_daily")
    return {
        "fact_inference": fact,
        "gold_cost_daily": cost_daily,
        "gold_budget_vs_actual": spark.sql(BUDGET_SQL),
        "gold_slo_monthly": spark.sql(SLO_SQL),
        "gold_model_performance": spark.sql(MODEL_PERF_SQL),
        "gold_dq_daily": spark.sql(DQ_DAILY_SQL),
        "gold_quarantine_summary": spark.sql(QUARANTINE_SUMMARY_SQL),
    }
