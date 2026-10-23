-- Q4: which apps breach their availability SLO, and by how much?
select
    s.app_id, a.department_id, s.month, s.total, s.failed,
    s.availability_pct, s.target_pct,
    s.availability_pct - s.target_pct as margin_pct,
    s.breached
from {{ ref('stg_gold_slo_monthly') }} s
join {{ ref('stg_dim_apps') }} a using (app_id)
