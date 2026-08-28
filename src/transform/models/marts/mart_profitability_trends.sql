{{ config(
    materialized = 'table',
    tags         = ['marts'],
    partition_by = {
        'field': 'hour_bucket',
        'data_type': 'timestamp',
        'granularity': 'day'
    },
    cluster_by   = ['gpu_model_name', 'offer_type']
) }}
with joined as (
    select
        f.offer_type,
        f.gpu_architecture,
        f.gpu_model_name,
        f.gpu_memory_gb,
        f.gpu_tdp_watts,
        f.gpu_bandwidth_gbytes_per_sec,
        f.gpu_max_cuda_version_supported,
        f.number_of_gpus,
        f.valid_from,
        f.revenue_usd_per_hr,
        f.profit_usd_per_hr,
        f.profit_per_tflop_usd,
        f.tariff_tier_skey,
        f.consumer_category,
        f.tariff_window_type,
        f.tariff_block_number

    from {{ ref('fct_compute_offers') }} f
    join {{ ref('dim_electricity_tariff_window_schedule') }} ts
        on  mod(extract(dayofweek from f.valid_from) + 5, 7) + 1 = ts.day_of_week
        and extract(hour from f.valid_from)                       = ts.hour
        and cast(f.valid_from as date) >= ts.valid_from
        and cast(f.valid_from as date) <  ts.valid_to
        and f.tariff_window_type = ts.tariff_window_type
)

select
    -- time bucket
    timestamp_trunc(valid_from, hour)                             as hour_bucket,
    mod(extract(dayofweek from valid_from) + 5, 7) + 1           as day_of_week,
    extract(hour from valid_from)                                 as hour_of_day,

    -- offer / gpu
    offer_type,
    gpu_architecture,
    gpu_model_name,
    gpu_memory_gb,
    gpu_tdp_watts,
    gpu_bandwidth_gbytes_per_sec,
    gpu_max_cuda_version_supported,
    number_of_gpus,
    count(*)                                                      as offer_count,

    -- revenue (USD/hr)
    avg(revenue_usd_per_hr)                                       as avg_revenue_usd_per_hr,

    -- profitability (USD/hr)
    avg(profit_usd_per_hr)                                        as avg_profit_usd_per_hr,
    min(profit_usd_per_hr)                                        as min_profit_usd_per_hr,
    max(profit_usd_per_hr)                                        as max_profit_usd_per_hr,

    -- profitability per TFLOP (USD)
    avg(profit_per_tflop_usd)                                     as avg_profit_per_tflop_usd,
    min(profit_per_tflop_usd)                                     as min_profit_per_tflop_usd,
    max(profit_per_tflop_usd)                                     as max_profit_per_tflop_usd,

    -- tariff tier context
    tariff_tier_skey,
    consumer_category,
    tariff_window_type as scheduled_tariff_window_type,
    tariff_block_number

from joined
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 20, 21, 22, 23
