-- Q5: which days can be trusted for reporting / internal billing?
-- A day is reliable when its valid rate reaches the agreed threshold (var reliable_valid_rate_pct).
select
    arrival_date, lines_received, accepted, duplicates_dropped, late_quarantined, invalid_quarantined,
    valid_rate_pct, late_arrivals,
    valid_rate_pct >= {{ var('reliable_valid_rate_pct') }} as reliable_for_reporting
from {{ ref('stg_gold_dq_daily') }}
