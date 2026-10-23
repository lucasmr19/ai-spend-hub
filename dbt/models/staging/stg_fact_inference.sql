-- user_id never leaves this model in clear: only the salted hash is exposed downstream.
select
    event_id, event_ts, event_date, app_id, department_id, model_id,
    {{ mask_id('user_id') }} as user_hash,
    request_type, tokens_in, tokens_out, latency_total_ms, ttft_ms,
    cache_status, status, error_code, cost_units, is_billable, is_cache_hit, arrival_date, lag_days
from {{ source('raw', 'fact_inference') }}
