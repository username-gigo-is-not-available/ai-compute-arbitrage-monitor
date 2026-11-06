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
    gpu_memory_gb,
    tariff_tier_skey,
    consumer_category,
    tariff_window_type,
    tariff_block_number,

    -- specs: hosts report TFLOPS per machine and one card varies, so the variant takes the median, like
    -- mart_gpu_history (#53). A reported 0 is no value.
    approx_quantiles(nullif(tflops_per_gpu, 0), 100)[offset(50)] as tflops_per_gpu,

    -- supply
    count(*) as number_of_machines,
    sum(number_of_machine_gpus) as total_number_of_machine_gpus,

    -- efficiency
    avg(kwh_per_tflop) as avg_kwh_per_tflop,

    -- revenue / profitability (USD/hr): medians, like mart_gpu_history and mart_gpu_forecast, since a few
    -- overpriced listings pull an average up (#53)
    approx_quantiles(revenue_per_gpu_usd_per_hr, 100)[offset(50)] as median_revenue_per_gpu_usd_per_hr,
    approx_quantiles(if(rentable_flag, revenue_per_gpu_usd_per_hr, null), 100)[offset(50)]
        as median_available_revenue_per_gpu_usd_per_hr,
    approx_quantiles(if(not rentable_flag, revenue_per_gpu_usd_per_hr, null), 100)[offset(50)]
        as median_taken_revenue_per_gpu_usd_per_hr,

    round(100.0 * countif(profit_usd_per_hr > 0) / nullif(count(*), 0), 2)
        as pct_profitable,

    -- host / flags
    round(100.0 * countif(verification_flag = 'verified') / nullif(count(*), 0), 2)
        as pct_verified,
    round(100.0 * countif(rentable_flag) / nullif(count(*), 0), 2) as pct_available,
    avg(reliability_score) as avg_reliability_score,

    -- time
    max(valid_from) as last_seen_at

from {{ ref('fct_compute_offers') }}
where cast(valid_to as date) = date '9999-12-31'
group by
    gpu_architecture,
    gpu_model_name,
    gpu_memory_gb,
    tariff_tier_skey,
    consumer_category,
    tariff_window_type,
    tariff_block_number
