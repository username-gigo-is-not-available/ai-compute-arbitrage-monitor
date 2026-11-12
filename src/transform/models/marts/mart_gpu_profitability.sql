{{ config(
    materialized = 'table',
    tags         = ['marts'],
    cluster_by   = ['gpu_architecture', 'gpu_model_name']
) }}
-- Which GPU is worth hosting: the typical machine of each variant, not its best listing, since the best is often an
-- unsold overpriced ask (a 4090 at $40/hr on 10-21). Available and taken machines both count, as do unverified ones;
-- a new host starts unverified. Ranking is left to the reader (the API), with number_of_machines to judge how solid
-- each median is.
with current_machines as (
    select
        *,
        profit_usd_per_hr / nullif(number_of_machine_gpus, 0) as profit_per_gpu_usd_per_hr,
        cost_usd_per_hr / nullif(number_of_machine_gpus, 0) as cost_per_gpu_usd_per_hr
    from {{ ref('fct_compute_offers') }}
    where cast(valid_to as date) = date '9999-12-31'
)

select
    gpu_architecture,
    gpu_model_name,
    gpu_memory_gb,
    tariff_tier_skey,
    consumer_category,
    tariff_window_type,
    tariff_block_number,

    -- A reported 0 is no value; the machine still counts for price and profit.
    approx_quantiles(nullif(tflops_per_gpu, 0), 100)[offset(50)] as tflops_per_gpu,
    approx_quantiles(gpu_tdp_watts, 100)[offset(50)] as gpu_tdp_watts,

    count(*) as number_of_machines,

    approx_quantiles(revenue_per_gpu_usd_per_hr, 100)[offset(50)] as market_ask_per_gpu_usd_per_hr,
    approx_quantiles(if(rentable_flag, revenue_per_gpu_usd_per_hr, null), 100)[offset(50)]
        as available_market_ask_per_gpu_usd_per_hr,
    approx_quantiles(if(not rentable_flag, revenue_per_gpu_usd_per_hr, null), 100)[offset(50)]
        as taken_market_ask_per_gpu_usd_per_hr,

    approx_quantiles(cost_per_gpu_usd_per_hr, 100)[offset(50)] as median_cost_per_gpu_usd_per_hr,
    -- The median of each machine's profit, as in mart_gpu_history, not median revenue minus median cost.
    approx_quantiles(profit_per_gpu_usd_per_hr, 100)[offset(50)] as median_profit_per_gpu_usd_per_hr,
    approx_quantiles(profit_per_tflop_usd, 100)[offset(50)] as median_profit_per_tflop_usd,
    round(100.0 * countif(profit_usd_per_hr > 0) / count(*), 2) as pct_profitable,

    max(valid_from) as valid_from
from current_machines
group by
    gpu_architecture,
    gpu_model_name,
    gpu_memory_gb,
    tariff_tier_skey,
    consumer_category,
    tariff_window_type,
    tariff_block_number
