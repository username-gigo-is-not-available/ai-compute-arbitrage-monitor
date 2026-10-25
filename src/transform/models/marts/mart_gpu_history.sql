{{ config(
    materialized = 'table',
    tags         = ['marts'],
    partition_by = {
        'field': 'census_date',
        'data_type': 'date',
        'granularity': 'day'
    },
    cluster_by   = ['gpu_model_name', 'gpu_memory_gb']
) }}

-- The past-facing pair of mart_gpu_forecast: GPU variant (model + memory) x hour x tariff tier (#40). The forecast
-- is still per model; #53 brings it to this grain. The census is daily (ADR-020), so each census's machines are priced at every local hour of their census
-- date, with the tariffs valid on that date. Every machine counts, available or taken. Values are per GPU, since
-- machines differ in size; TDP, TFLOPS and ask are medians across the variant's machines, profit is spread
-- across them.
with census_offers as (
    select
        *,
        valid_from                     as census_at,
        {{ evn_date('valid_from') }}   as census_date
    from {{ ref('fct_compute_offers') }}
),

census_hours as (
    select distinct
        census_at,
        census_date,
        history_hour,
        {{ evn_day_of_week('history_hour') }} as day_of_week,
        {{ evn_hour('history_hour') }}        as hour_of_day
    from census_offers
    cross join unnest(
        generate_timestamp_array(
            timestamp(census_date, '{{ var("evn_timezone") }}'),
            timestamp_sub(
                timestamp(date_add(census_date, interval 1 day), '{{ var("evn_timezone") }}'),
                interval 1 hour
            ),
            interval 1 hour
        )
    ) as history_hour
),

-- Each machine's tier rows are kept only at the hours whose EVN window is the tier's window.
priced as (
    select
        ch.census_at,
        ch.census_date,
        ch.history_hour,
        ch.day_of_week,
        ch.hour_of_day,
        o.tariff_window_type,
        o.gpu_architecture,
        o.gpu_model_name,
        o.gpu_memory_gb,
        o.gpu_tdp_watts,
        o.tflops_per_gpu,
        o.number_of_machine_gpus,
        o.revenue_per_gpu_usd_per_hr,
        o.profit_usd_per_hr / nullif(o.number_of_machine_gpus, 0) as profit_per_gpu_usd_per_hr,
        o.profit_per_tflop_usd,
        o.tariff_tier_skey,
        o.consumer_category,
        o.tariff_block_number
    from census_offers o
    join census_hours ch
        on ch.census_at = o.valid_from
    join {{ ref('dim_electricity_tariff_window_schedule') }} ts
        on  ts.day_of_week        = ch.day_of_week
        and ts.hour               = ch.hour_of_day
        and ts.tariff_window_type = o.tariff_window_type
        and ch.census_date >= ts.valid_from
        and ch.census_date <  ts.valid_to
)

select
    -- time
    census_at,
    census_date,
    history_hour,
    day_of_week,
    hour_of_day,
    tariff_window_type                                                   as scheduled_tariff_window_type,

    -- gpu variant
    gpu_architecture,
    gpu_model_name,
    gpu_memory_gb,
    approx_quantiles(nullif(gpu_tdp_watts, 0), 100)[offset(50)]          as gpu_tdp_watts,
    approx_quantiles(nullif(tflops_per_gpu, 0), 100)[offset(50)]         as tflops_per_gpu,

    -- supply
    count(*)                                                             as number_of_machines,
    sum(number_of_machine_gpus)                                          as total_number_of_machine_gpus,

    -- revenue (USD/hr per GPU)
    approx_quantiles(revenue_per_gpu_usd_per_hr, 100)[offset(50)]        as market_ask_per_gpu_usd_per_hr,

    -- profit per GPU (USD/hr): electricity only, the access fee is paid anyway (ADR-025). The median is the typical
    -- machine; the average is pulled up by a few overpriced listings (a 4090 at $40/hr on 10-21).
    approx_quantiles(profit_per_gpu_usd_per_hr, 100)[offset(50)]         as median_profit_per_gpu_usd_per_hr,
    avg(profit_per_gpu_usd_per_hr)                                       as avg_profit_per_gpu_usd_per_hr,
    min(profit_per_gpu_usd_per_hr)                                       as min_profit_per_gpu_usd_per_hr,
    max(profit_per_gpu_usd_per_hr)                                       as max_profit_per_gpu_usd_per_hr,

    -- profit per TFLOP (USD)
    approx_quantiles(profit_per_tflop_usd, 100)[offset(50)]              as median_profit_per_tflop_usd,
    avg(profit_per_tflop_usd)                                            as avg_profit_per_tflop_usd,
    min(profit_per_tflop_usd)                                            as min_profit_per_tflop_usd,
    max(profit_per_tflop_usd)                                            as max_profit_per_tflop_usd,

    -- tariff tier context
    tariff_tier_skey,
    consumer_category,
    tariff_block_number

from priced
group by
    census_at,
    census_date,
    history_hour,
    day_of_week,
    hour_of_day,
    scheduled_tariff_window_type,
    gpu_architecture,
    gpu_model_name,
    gpu_memory_gb,
    tariff_tier_skey,
    consumer_category,
    tariff_block_number
