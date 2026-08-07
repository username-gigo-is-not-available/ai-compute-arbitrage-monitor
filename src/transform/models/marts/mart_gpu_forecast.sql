{{ config(
    materialized = 'table',
    tags         = ['marts'],
    cluster_by   = ['gpu_model_name', 'consumer_category']
) }}

-- ===========================================================================
-- mart_gpu_forecast
-- ---------------------------------------------------------------------------
-- Forward-looking, per-machine, tariff-window-aware forecast of electricity
-- cost and profit for a host who already owns a GPU and rents it on Vast.ai.
--
-- Grain: one row per (gpu_model_name, forecast_hour, consumer_category).
--
-- The future-hours spine is generated and joined to the tariff window
-- schedule (is_latest) so each hour carries the correct low/high window.
-- Revenue defaults to the current market ask for that GPU model. Cost is
-- electricity-only: (kwh_per_hr * (tariff_value + distribution_fee)) / rate,
-- deliberately EXCLUDING the fixed monthly access fee (a sunk cost for the
-- marginal "should I run my GPU this window?" decision).
--
-- The ask price can be overridden at build time via the
-- forecast_ask_price_usd_per_hr var; when unset, the market ask is used.
-- ===========================================================================

{% set forecast_days = var('forecast_days', 7) %}
{% set consumer_category = var('forecast_consumer_category', 'household') %}
{% set ask_override = var('forecast_ask_price_usd_per_hr', none) %}

-- ---------------------------------------------------------------------------
-- 1. Future-hours spine
-- ---------------------------------------------------------------------------
with hours_spine as (
    select
        forecast_hour,
        mod(extract(dayofweek from forecast_hour) + 5, 7) + 1 as day_of_week,
        extract(hour from forecast_hour)                       as hour
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

-- ---------------------------------------------------------------------------
-- 2. Tariff window schedule (latest version) - the weekly low/high pattern
-- ---------------------------------------------------------------------------
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
        s.tariff_window_type
    from hours_spine hs
    join schedule s
        on  hs.day_of_week = s.day_of_week
        and hs.hour        = s.hour
),

-- ---------------------------------------------------------------------------
-- 3. Representative GPU specs per model (most recent current offer)
-- ---------------------------------------------------------------------------
gpu_specs as (
    select
        gpu_model_name,
        gpu_tdp_watts,
        number_of_gpus,
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

-- ---------------------------------------------------------------------------
-- 4. Market ask price per GPU model (median current offer revenue)
-- ---------------------------------------------------------------------------
market_revenue as (
    select
        gpu_model_name,
        approx_quantiles(revenue_usd_per_hr, 100)[offset(50)] as market_ask_usd_per_hr
    from {{ ref('fct_compute_offers') }}
    where cast(valid_to as date) = date '9999-12-31'
      and revenue_usd_per_hr > 0
    group by gpu_model_name
),

-- ---------------------------------------------------------------------------
-- 5. Tariff tier context for the chosen consumer category + window
--    Pick the current tier with the lowest block (NULL block first covers
--    household-low / business tiers that have no block distinction).
-- ---------------------------------------------------------------------------
tariff_tiers as (
    select
        consumer_category,
        tariff_window_type,
        tariff_value,
        tariff_block_number,
        tariff_tier_skey
    from {{ ref('dim_electricity_tariff_tiers') }}
    where is_latest = true
    qualify row_number() over (
        partition by consumer_category, tariff_window_type
        order by tariff_block_number nulls first
    ) = 1
),

-- ---------------------------------------------------------------------------
-- 6. Distribution fee (per-kWh) per consumer category
--    Access fee deliberately EXCLUDED - sunk monthly cost, not marginal.
-- ---------------------------------------------------------------------------
fees as (
    select
        consumer_category,
        max(case when fee_type = 'distribution' then value end) as distribution_fee
    from {{ ref('dim_electricity_tariff_fees') }}
    where is_latest = true
    group by consumer_category
),

-- ---------------------------------------------------------------------------
-- 7. Latest USD/MKD exchange rate
-- ---------------------------------------------------------------------------
exchange_rate as (
    select value as usd_to_mkd_rate
    from {{ ref('dim_exchange_rates') }}
    where from_currency = 'USD'
      and to_currency   = 'MKD'
    qualify row_number() over (order by valid_from desc) = 1
),

-- ---------------------------------------------------------------------------
-- 8. Assemble
-- ---------------------------------------------------------------------------
combined as (
    select
        sw.forecast_hour,
        sw.tariff_window_type,
        gs.gpu_model_name,
        gs.gpu_tdp_watts,
        gs.number_of_gpus,
        gs.tflops_per_gpu,
        gs.gpu_memory_gb,
        gs.gpu_bandwidth_gbytes_per_sec,
        gs.gpu_max_cuda_version_supported,
        mr.market_ask_usd_per_hr,
        tt.tariff_tier_skey,
        tt.tariff_value,
        tt.tariff_block_number,
        ft.distribution_fee,
        er.usd_to_mkd_rate
    from spine_with_window sw
    cross join gpu_specs gs
    join market_revenue mr
        on gs.gpu_model_name = mr.gpu_model_name
    join tariff_tiers tt
        on  tt.consumer_category = '{{ consumer_category }}'
        and tt.tariff_window_type = sw.tariff_window_type
    join fees ft
        on ft.consumer_category = tt.consumer_category
    cross join exchange_rate er
),

-- ---------------------------------------------------------------------------
-- 9. Compute metrics
-- ---------------------------------------------------------------------------
metrics as (
    select
        *,
        (gpu_tdp_watts * number_of_gpus) / 1000.0 as total_system_kwh_per_hr,
        -- ask price: user override if provided, else market ask
        {% if ask_override is not none %}
            cast({{ ask_override }} as float64) as forecast_ask_usd_per_hr,
        {% else %}
            market_ask_usd_per_hr as forecast_ask_usd_per_hr,
        {% endif %}
        -- total compute across all GPUs in the machine
        tflops_per_gpu * number_of_gpus as total_system_tflops,
        -- electricity-only cost, access fee excluded
        ((gpu_tdp_watts * number_of_gpus) / 1000.0
            * (tariff_value + coalesce(distribution_fee, 0)))
            / nullif(usd_to_mkd_rate, 0) as cost_usd_per_hr
    from combined
)

select
    forecast_hour,
    mod(extract(dayofweek from forecast_hour) + 5, 7) + 1         as day_of_week,
    extract(hour from forecast_hour)                               as hour_of_day,
    tariff_window_type                                             as scheduled_tariff_window_type,

    gpu_model_name,
    gpu_tdp_watts,
    number_of_gpus,
    tflops_per_gpu,
    gpu_memory_gb,
    gpu_bandwidth_gbytes_per_sec,
    gpu_max_cuda_version_supported,

    -- pricing
    market_ask_usd_per_hr,
    forecast_ask_usd_per_hr,
    cost_usd_per_hr,
    forecast_ask_usd_per_hr - cost_usd_per_hr                     as profit_usd_per_hr,
    cost_usd_per_hr / nullif(total_system_tflops, 0)              as cost_per_tflop_usd,
    (forecast_ask_usd_per_hr - cost_usd_per_hr)
        / nullif(total_system_tflops, 0)                          as profit_per_tflop_usd,

    total_system_kwh_per_hr,
    total_system_tflops,

    -- tariff tier context
    tariff_tier_skey,
    '{{ consumer_category }}'                                      as consumer_category,
    tariff_block_number,
    tariff_value,
    distribution_fee,
    usd_to_mkd_rate

from metrics
