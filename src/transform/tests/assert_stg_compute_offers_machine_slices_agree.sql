-- Test: every offer (slice) of a machine in one census agrees on machine size and per-GPU price (ADR-020).
-- int_compute_offers collapses a machine's offers into one row on these two facts; verified on the first full
-- census (2026-10-05: 3,294 multi-slice machines, 0 disagreements), checked here on every census.
-- Snapshots without gpu_fraction_of_machine predate the census and are not collapsed, so they are skipped.
-- Returns any (machine_id, snapshot_at) whose offers disagree.

select
    machine_id,
    snapshot_at,
    count(distinct round(number_of_gpus / gpu_fraction_of_machine))   as machine_sizes,
    count(distinct round(gpu_price_usd_per_hr / number_of_gpus, 4))   as per_gpu_prices
from {{ ref('stg_compute_offers') }}
where gpu_fraction_of_machine is not null
  and offer_type = 'on_demand'
group by machine_id, snapshot_at
having machine_sizes > 1 or per_gpu_prices > 1
