{{ config(severity = 'warn') }}

-- Test: every offer (slice) of a machine in one census agrees on GPU model, machine size and per-GPU price (ADR-020).
-- int_compute_offers collapses a machine's offers into one row on these facts and drops the machines whose offers
-- disagree, so this warns with the machines dropped: 19, 20 and 13 in the 2026-10-05/12/21 censuses, machines that
-- mix GPU models (e.g. a 5090 next to four 4090s).
-- Returns any (machine_id, snapshot_at) whose offers disagree.

select
    machine_id,
    snapshot_at,
    count(distinct gpu_model_name)                                          as gpu_models,
    count(distinct {{ number_of_machine_gpus() }})                          as machine_sizes,
    count(distinct round(gpu_price_usd_per_hr / number_of_offer_gpus, 4))   as per_gpu_prices
from {{ ref('stg_compute_offers') }}
group by machine_id, snapshot_at
having gpu_models > 1 or machine_sizes > 1 or per_gpu_prices > 1
