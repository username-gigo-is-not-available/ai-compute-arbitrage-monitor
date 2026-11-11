{{ config(
    materialized = 'table',
    tags         = ['marts'],
    cluster_by   = ['gpu_model_name']
) }}
-- The market of each GPU variant over the latest census, counting machines, not their overlapping offers (ADR-020).
-- Nothing here depends on the tariff, so there is no tier; prices and profit are in mart_gpu_profitability (ADR-026).
with current_machines as (
    -- The fact has one row per machine and tariff tier; the market counts each machine once.
    select distinct
        machine_id,
        gpu_architecture,
        gpu_model_name,
        gpu_memory_gb,
        tflops_per_gpu,
        number_of_machine_gpus,
        kwh_per_tflop,
        verification_flag,
        rentable_flag,
        reliability_score,
        valid_from
    from {{ ref('fct_compute_offers') }}
    where cast(valid_to as date) = date '9999-12-31'
)

select
    gpu_architecture,
    gpu_model_name,
    gpu_memory_gb,

    -- Hosts report TFLOPS per machine and one card varies, so the variant takes the median (#53). A reported 0 is
    -- no value.
    approx_quantiles(nullif(tflops_per_gpu, 0), 100)[offset(50)] as tflops_per_gpu,

    count(*) as number_of_machines,
    sum(number_of_machine_gpus) as total_number_of_machine_gpus,

    avg(kwh_per_tflop) as avg_kwh_per_tflop,

    round(100.0 * countif(verification_flag = 'verified') / nullif(count(*), 0), 2)
        as pct_verified,
    round(100.0 * countif(rentable_flag) / nullif(count(*), 0), 2) as pct_available,
    avg(reliability_score) as avg_reliability_score,

    max(valid_from) as last_seen_at

from current_machines
group by
    gpu_architecture,
    gpu_model_name,
    gpu_memory_gb
