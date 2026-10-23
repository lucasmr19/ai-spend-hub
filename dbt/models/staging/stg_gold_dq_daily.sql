select * from {{ source('raw', 'gold_dq_daily') }}
