{{ config(severity = 'warn') }}

-- Test: every machine in a census reports a GPU power draw. Cost is TDP x tariff, so int_compute_offers drops the
-- machines without one (#53); this warns with them: machine 152661 (RTX 3080, 0 W) in the 2026-10-12/21 censuses.
-- Returns any (machine_id, snapshot_at) with a missing or zero TDP.

select
    machine_id,
    snapshot_at,
    max(gpu_tdp_watts) as max_gpu_tdp_watts
from {{ ref('stg_compute_offers') }}
group by machine_id, snapshot_at
having coalesce(max(gpu_tdp_watts), 0) <= 0
