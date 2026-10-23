-- Q3: how much did caching save?  (cache_savings = what the hit would have cost as a billable call)
select
    substr(cast(event_date as varchar), 1, 7) as month,
    app_id,
    sum(requests)                              as requests,
    sum(cache_hits)                            as cache_hits,
    sum(cache_hits) * 100.0 / sum(requests)    as hit_rate_pct,
    sum(cost_units)                            as cost_units,
    sum(cache_savings_units)                   as cache_savings_units,
    sum(cost_eur)                              as cost_eur,
    sum(cache_savings_eur)                     as cache_savings_eur  -- exact decimals, no float division
from {{ ref('stg_gold_cost_daily') }}
group by 1, 2
