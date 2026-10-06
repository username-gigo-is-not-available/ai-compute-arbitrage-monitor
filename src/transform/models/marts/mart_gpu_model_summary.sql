{{ config(
    materialized = 'table',
    tags         = ['marts'],
    cluster_by   = ['gpu_model_name']
) }}
-- One row per GPU variant and tariff tier over the latest census, which counts machines, not their overlapping
-- offers (ADR-020). Taken machines show prices being paid; available machines show the competition (CONTEXT.md).
select
    -- identity / grouping
    gpu_architecture,
    gpu_model_name,
    tflops_per_gpu,
    gpu_memory_gb,
    tariff_tier_skey,
    consumer_category,
    tariff_window_type,
    tariff_block_number,

    -- supply
    count(*)                                                                     as machine_count,
    sum(number_of_machine_gpus)                                                  as gpu_count,

    -- efficiency
    avg(kwh_per_tflop)                                                           as avg_kwh_per_tflop,

    -- revenue / profitability (USD/hr)
    avg(revenue_usd_per_hr / nullif(number_of_machine_gpus, 0))                  as avg_revenue_per_gpu_usd_per_hr,
    avg(case when rentable_flag then revenue_usd_per_hr / nullif(number_of_machine_gpus, 0) end)
                                                                                 as avg_available_revenue_per_gpu_usd_per_hr,
    avg(case when not rentable_flag then revenue_usd_per_hr / nullif(number_of_machine_gpus, 0) end)
                                                                                 as avg_taken_revenue_per_gpu_usd_per_hr,

    round(100.0 * countif(profit_usd_per_hr > 0) / nullif(count(*), 0), 2)      as pct_profitable,

    -- host / flags
    round(100.0 * countif(verification_flag = 'verified') / nullif(count(*), 0), 2)
                                                                                 as pct_verified,
    round(100.0 * countif(rentable_flag) / nullif(count(*), 0), 2)              as pct_available,
    avg(reliability_score)                                                       as avg_reliability_score,

    -- time
    max(valid_from)                                                              as last_seen_at

from {{ ref('fct_compute_offers') }}
where cast(valid_to as date) = date '9999-12-31'
group by
    gpu_architecture,
    gpu_model_name,
    tflops_per_gpu,
    gpu_memory_gb,
    tariff_tier_skey,
    consumer_category,
    tariff_window_type,
    tariff_block_number
