-- Internal billing: billable cost per department-month. A month is "billable_ready" only when it is
-- over (a later month exists) and every arrival day of that month met the reliability threshold.
with cost as (
    select department_id, month, cost_eur, cost_units from {{ ref('stg_gold_budget_vs_actual') }}
),
rel as (
    select substr(cast(arrival_date as varchar), 1, 7) as month,
           min(case when reliable_for_reporting then 1 else 0 end) = 1 as all_days_reliable
    from {{ ref('mart_data_reliability') }} group by 1
),
last_month as (select max(month) as m from cost)
select
    c.department_id, c.month, c.cost_units, c.cost_eur,
    c.month < (select m from last_month) and coalesce(r.all_days_reliable, false) as billable_ready
from cost c left join rel r using (month)
