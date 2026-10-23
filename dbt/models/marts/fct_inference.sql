-- Event-level fact with the pseudonymised user (no clear user_id anywhere in this schema).
select * from {{ ref('stg_fact_inference') }}
