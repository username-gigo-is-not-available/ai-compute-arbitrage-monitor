{{ config(severity = 'warn') }}

-- Every machine in a census asks a price. A $0 ask is no price, so int_compute_offers drops the machines without
-- one; this warns with them.

select
    machine_id,
    snapshot_at,
    max(total_price_usd_per_hr) as max_total_price_usd_per_hr
from {{ ref('stg_compute_offers') }}
group by machine_id, snapshot_at
having coalesce(max(total_price_usd_per_hr), 0) <= 0
