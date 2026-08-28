{{ config(
    materialized = 'table',
    tags         = ['marts'],
    cluster_by   = ['offer_type', 'gpu_architecture', 'gpu_model_name']
) }}
with current_offers as (
    select * from {{ ref('fct_compute_offers') }}
    where cast(valid_to as date) = date '9999-12-31'
),

available_offers as (
    select * from current_offers
    where
        coalesce(total_system_tflops, 0) > 0
        and coalesce(revenue_usd_per_hr, 0) > 0
        and coalesce(gpu_tdp_watts, 0) > 0
        and rented_flag = false
        and rentable_flag = true
        and verification_flag = 'verified'
),

normalized_offers as (
    select
        *,
        row_number() over (
            partition by offer_type, gpu_architecture, gpu_model_name, gpu_memory_gb, tariff_tier_skey
            order by valid_from desc, (profit_usd_per_hr / nullif(number_of_gpus, 0)) desc
        ) as rn
    from available_offers
)

select
    -- offer
    offer_type,

    -- gpu
    gpu_architecture,
    gpu_model_name,
    gpu_memory_gb,
    gpu_tdp_watts,
    gpu_bandwidth_gbytes_per_sec,
    gpu_max_cuda_version_supported,
    tflops_per_gpu,

    -- efficiency
    kwh_per_tflop,

    -- host
    verification_flag,
    rentable_flag,
    rented_flag,
    reliability_score,
    country_code,

    -- revenue / cost / profit (USD/hr per GPU)
    revenue_usd_per_hr / nullif(number_of_gpus, 0)                  as revenue_per_gpu_usd_per_hr,
    cost_usd_per_hr / nullif(number_of_gpus, 0)                     as cost_per_gpu_usd_per_hr,
    profit_usd_per_hr / nullif(number_of_gpus, 0)                   as profit_per_gpu_usd_per_hr,

    -- per TFLOP (USD)
    cost_per_tflop_usd,
    profit_per_tflop_usd,

    -- tariff tier context
    tariff_tier_skey,
    consumer_category,
    tariff_window_type,
    tariff_block_number,

    -- time
    valid_from
from normalized_offers
where rn = 1