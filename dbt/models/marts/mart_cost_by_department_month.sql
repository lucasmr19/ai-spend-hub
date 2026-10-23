-- Q1: what does each department spend, against its budget, and how is it trending?
with b as (select * from {{ ref('stg_gold_budget_vs_actual') }}),
d as (select department_id, department_name from {{ ref('stg_dim_departments') }})
select
    b.department_id,
    d.department_name,
    b.month,
    b.cost_eur,
    b.budget_eur,
    b.utilisation_pct,
    b.over_budget,
    b.cost_eur - lag(b.cost_eur) over (partition by b.department_id order by b.month) as mom_change_eur
from b join d using (department_id)
