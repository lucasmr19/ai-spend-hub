-- Q2: which model is best per request type on cost, latency and quality?
-- pareto_efficient = no other model of the same request type is at least as good on all three
-- axes and strictly better on one.
with m as (
    select
        model_id, request_type,
        sum(invocations) as invocations,
        sum(cost_units)  as cost_units,
        sum(cost_units) * 1000.0 / sum(invocations) / 100000000.0 as cost_per_1k_eur,
        sum(latency_p95_ms * invocations) / sum(invocations)       as latency_p95_ms_avg,
        max(quality_score)                                         as quality_score
    from {{ ref('stg_gold_model_performance') }}
    group by model_id, request_type
)
select
    a.*,
    not exists (
        select 1 from m b
        where b.request_type = a.request_type and b.model_id <> a.model_id
          and b.cost_per_1k_eur <= a.cost_per_1k_eur
          and b.latency_p95_ms_avg <= a.latency_p95_ms_avg
          and b.quality_score >= a.quality_score
          and (b.cost_per_1k_eur < a.cost_per_1k_eur or b.latency_p95_ms_avg < a.latency_p95_ms_avg
               or b.quality_score > a.quality_score)
    ) as pareto_efficient
from m a
