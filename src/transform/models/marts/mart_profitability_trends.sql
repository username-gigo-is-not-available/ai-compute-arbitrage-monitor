{{ config(
    materialized = 'table',
    tags         = ['marts'],
    partition_by = {
        'field': 'hour_bucket',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by   = ['gpu_model_name']
) }}
with joined as (
    select
        f.gpu_architecture,
        f.gpu_model_name,
        f.gpu_memory_gb,
        f.gpu_tdp_watts,
        f.gpu_bandwidth_gbytes_per_sec,
        f.gpu_max_cuda_version_supported,
        f.number_of_machine_gpus,
        f.valid_from,
        f.revenue_usd_per_hr,
        f.marginal_profit_usd_per_hr,
        f.average_profit_usd_per_hr,
        f.marginal_profit_per_tflop_usd,
        f.average_profit_per_tflop_usd,
        f.tariff_tier_skey,
        f.consumer_category,
        f.tariff_window_type,
        f.tariff_block_number

    from {{ ref('fct_compute_offers') }} f
    join {{ ref('dim_electricity_tariff_window_schedule') }} ts
        on  {{ evn_day_of_week('f.valid_from') }} = ts.day_of_week
        and {{ evn_hour('f.valid_from') }}        = ts.hour
        and cast(f.valid_from as date) >= ts.valid_from
        and cast(f.valid_from as date) <  ts.valid_to
        and f.tariff_window_type = ts.tariff_window_type
)

select
    -- time bucket
    timestamp_trunc(valid_from, hour)                             as hour_bucket,
    {{ evn_day_of_week('valid_from') }}                           as day_of_week,
    {{ evn_hour('valid_from') }}                                  as hour_of_day,

    -- gpu
    gpu_architecture,
    gpu_model_name,
    gpu_memory_gb,
    gpu_tdp_watts,
    gpu_bandwidth_gbytes_per_sec,
    gpu_max_cuda_version_supported,
    number_of_machine_gpus,
    count(*)                                                      as number_of_machines,

    -- revenue (USD/hr)
    avg(revenue_usd_per_hr)                                       as avg_revenue_usd_per_hr,

    -- profitability (USD/hr): marginal excludes the access fee, average includes it (ADR-021)
    avg(marginal_profit_usd_per_hr)                               as avg_marginal_profit_usd_per_hr,
    min(marginal_profit_usd_per_hr)                               as min_marginal_profit_usd_per_hr,
    max(marginal_profit_usd_per_hr)                               as max_marginal_profit_usd_per_hr,
    avg(average_profit_usd_per_hr)                                as avg_average_profit_usd_per_hr,
    min(average_profit_usd_per_hr)                                as min_average_profit_usd_per_hr,
    max(average_profit_usd_per_hr)                                as max_average_profit_usd_per_hr,

    -- profitability per TFLOP (USD)
    avg(marginal_profit_per_tflop_usd)                            as avg_marginal_profit_per_tflop_usd,
    min(marginal_profit_per_tflop_usd)                            as min_marginal_profit_per_tflop_usd,
    max(marginal_profit_per_tflop_usd)                            as max_marginal_profit_per_tflop_usd,
    avg(average_profit_per_tflop_usd)                             as avg_average_profit_per_tflop_usd,
    min(average_profit_per_tflop_usd)                             as min_average_profit_per_tflop_usd,
    max(average_profit_per_tflop_usd)                             as max_average_profit_per_tflop_usd,

    -- tariff tier context
    tariff_tier_skey,
    consumer_category,
    tariff_window_type as scheduled_tariff_window_type,
    tariff_block_number

from joined
group by
    hour_bucket,
    day_of_week,
    hour_of_day,
    gpu_architecture,
    gpu_model_name,
    gpu_memory_gb,
    gpu_tdp_watts,
    gpu_bandwidth_gbytes_per_sec,
    gpu_max_cuda_version_supported,
    number_of_machine_gpus,
    tariff_tier_skey,
    consumer_category,
    scheduled_tariff_window_type,
    tariff_block_number
