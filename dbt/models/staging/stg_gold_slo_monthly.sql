select * from {{ source('raw', 'gold_slo_monthly') }}
