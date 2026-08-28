{{ config(
    materialized = 'table',
    tags         = ['marts'],
    cluster_by   = ['gpu_model_name', 'offer_type']
) }}
select
    -- identity / grouping
    offer_type,
    gpu_architecture,
    gpu_model_name,
    tflops_per_gpu,
    gpu_memory_gb,
    count(*)                                    as total_offer_observations,

    -- efficiency
    avg(kwh_per_tflop)                          as avg_kwh_per_tflop,

    -- revenue / profitability (USD/hr)
    avg(revenue_usd_per_hr / nullif(number_of_gpus, 0))                 as avg_revenue_per_gpu_usd_per_hr,

    round(100.0 * sum(case when profit_usd_per_hr > 0 then 1 else 0 end)
        / nullif(count(*), 0), 2)               as pct_profitable,

    -- host / flags
    round(100.0 * sum(case when verification_flag = 'verified' then 1 else 0 end)
        / nullif(count(*), 0), 2)               as pct_verified,
    round(100.0 * sum(case when rented_flag = false then 1 else 0 end)
        / nullif(count(*), 0), 2)               as pct_available,
    avg(reliability_score)                      as avg_reliability_score,

    -- tariff tier context
    tariff_tier_skey,
    consumer_category,
    tariff_window_type,
    tariff_block_number,

    -- time
    max(valid_from)                             as last_seen_at

from {{ ref('fct_compute_offers') }}
where cast(valid_to as date) = date '9999-12-31'
group by 1, 2, 3, 4, 5, 13, 14, 15, 16
