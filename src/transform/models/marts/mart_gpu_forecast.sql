{{ config(
    materialized = 'table',
    tags         = ['marts'],
    cluster_by   = ['gpu_model_name', 'consumer_category']
) }}

-- A per-GPU grid of GPU model x future hour x tariff tier (ADR-021). It carries every consumer category and tariff
-- block, and no per-user input: the app picks the user's category, walks the blocks with a running total from
-- their billing-period consumption, and scales by their GPU count and ask price.
-- forecast_days covers the app's 30-day window plus the gap between rebuilds.
{% set forecast_days = var('forecast_days', 32) %}


with hours_spine as (
    select
        forecast_hour,
        {{ evn_day_of_week('forecast_hour') }} as day_of_week,
        {{ evn_hour('forecast_hour') }}        as hour
    from unnest(
        generate_timestamp_array(
            timestamp_trunc(current_timestamp(), hour),
            timestamp_add(
                timestamp_trunc(current_timestamp(), hour),
                interval {{ forecast_days }} day
            ),
            interval 1 hour
        )
    ) as forecast_hour
),


schedule as (
    select
        day_of_week,
        hour,
        tariff_window_type
    from {{ ref('dim_electricity_tariff_window_schedule') }}
    where is_latest = true
),

spine_with_window as (
    select
        hs.forecast_hour,
        hs.day_of_week,
        hs.hour,
        s.tariff_window_type
    from hours_spine hs
    join schedule s
        on  hs.day_of_week = s.day_of_week
        and hs.hour        = s.hour
),


gpu_specs as (
    select
        gpu_model_name,
        gpu_tdp_watts,
        tflops_per_gpu,
        gpu_memory_gb,
        gpu_bandwidth_gbytes_per_sec,
        gpu_max_cuda_version_supported
    from {{ ref('fct_compute_offers') }}
    where cast(valid_to as date) = date '9999-12-31'
    qualify row_number() over (
        partition by gpu_model_name
        order by valid_from desc
    ) = 1
),


market_revenue as (
    -- Median per-GPU ask across the latest census's machines (ADR-020): machines differ in size, so a median of
    -- whole-machine prices would not be a per-GPU price.
    select
        gpu_model_name,
        approx_quantiles(revenue_per_gpu_usd_per_hr, 100)[offset(50)] as market_ask_per_gpu_usd_per_hr
    from {{ ref('fct_compute_offers') }}
    where cast(valid_to as date) = date '9999-12-31'
      and revenue_usd_per_hr > 0
    group by gpu_model_name
),


tariff_tiers as (
    select
        consumer_category,
        tariff_window_type,
        value as tariff_value,
        tariff_block_number,
        skey as tariff_tier_skey
    from {{ ref('dim_electricity_tariff_tiers') }}
    where is_latest = true
),

-- Bounds are for a 30-day billing period; the app scales them to the user's period length (ADR-021).
tariff_blocks as (
    select
        consumer_category,
        tariff_window_type,
        tariff_block_number,
        lower_bound_kwh,
        upper_bound_kwh
    from {{ ref('dim_electricity_tariff_blocks') }}
    where is_latest = true
),


fees as (
    select
        consumer_category,
        max(case when fee_type = 'distribution' then value end) as distribution_fee
    from {{ ref('dim_electricity_tariff_fees') }}
    where is_latest = true
    group by consumer_category
),


exchange_rate as (
    select value as usd_to_mkd_rate
    from {{ ref('dim_exchange_rates') }}
    where from_currency = 'USD'
      and to_currency   = 'MKD'
    qualify row_number() over (order by valid_from desc) = 1
),


combined as (
    select
        sw.forecast_hour,
        sw.day_of_week,
        sw.hour,
        sw.tariff_window_type,
        gs.gpu_model_name,
        gs.gpu_tdp_watts,
        gs.tflops_per_gpu,
        gs.gpu_memory_gb,
        gs.gpu_bandwidth_gbytes_per_sec,
        gs.gpu_max_cuda_version_supported,
        mr.market_ask_per_gpu_usd_per_hr,
        tt.tariff_tier_skey,
        tt.consumer_category,
        tt.tariff_value,
        tt.tariff_block_number,
        tb.lower_bound_kwh,
        tb.upper_bound_kwh,
        ft.distribution_fee,
        {{ vat_rate('tt.consumer_category') }} as vat_rate,
        er.usd_to_mkd_rate
    from spine_with_window sw
    cross join gpu_specs gs
    join market_revenue mr
        on gs.gpu_model_name = mr.gpu_model_name
    join tariff_tiers tt
        on tt.tariff_window_type = sw.tariff_window_type
    left join tariff_blocks tb
        on  tb.consumer_category   = tt.consumer_category
        and tb.tariff_window_type  = tt.tariff_window_type
        and tb.tariff_block_number = tt.tariff_block_number
    join fees ft
        on ft.consumer_category = tt.consumer_category
    cross join exchange_rate er
),


-- Marginal cost only: the access fee is paid whether or not the GPU runs (ADR-007, ADR-021).
metrics as (
    select
        *,
        gpu_tdp_watts / 1000.0 as kwh_per_gpu_per_hr,
        (gpu_tdp_watts / 1000.0 * (tariff_value + coalesce(distribution_fee, 0)))
            * (1 + vat_rate) / nullif(usd_to_mkd_rate, 0) as marginal_cost_per_gpu_usd_per_hr
    from combined
)

select
    forecast_hour,
    day_of_week,
    hour                                                           as hour_of_day,
    tariff_window_type                                             as scheduled_tariff_window_type,

    gpu_model_name,
    gpu_tdp_watts,
    tflops_per_gpu,
    gpu_memory_gb,
    gpu_bandwidth_gbytes_per_sec,
    gpu_max_cuda_version_supported,

    kwh_per_gpu_per_hr,
    market_ask_per_gpu_usd_per_hr,
    marginal_cost_per_gpu_usd_per_hr,
    market_ask_per_gpu_usd_per_hr - marginal_cost_per_gpu_usd_per_hr as marginal_profit_per_gpu_usd_per_hr,
    marginal_cost_per_gpu_usd_per_hr / nullif(tflops_per_gpu, 0)     as marginal_cost_per_tflop_usd,
    (market_ask_per_gpu_usd_per_hr - marginal_cost_per_gpu_usd_per_hr)
        / nullif(tflops_per_gpu, 0)                                  as marginal_profit_per_tflop_usd,

    tariff_tier_skey,
    consumer_category,
    tariff_block_number,
    lower_bound_kwh,
    upper_bound_kwh,
    tariff_value,
    distribution_fee,
    vat_rate,
    usd_to_mkd_rate

from metrics
