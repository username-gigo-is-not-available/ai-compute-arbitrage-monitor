{{
    config(
        materialized     = 'incremental',
        unique_key       = ['machine_id', 'valid_from', 'tariff_tier_skey'],
        on_schema_change = 'append_new_columns',
        tags = ['compute_offers'],
        partition_by     = {
                'field': 'valid_from',
                'data_type': 'timestamp',
                'granularity': 'day'
            },
        cluster_by       = ['gpu_model_name', 'machine_id'],
        post_hook = """
            {% if execute %}
                {% set latest_ts_query %}
                    select max(valid_from) from {{ this }}
                {% endset %}

                {% set results = run_query(latest_ts_query) %}

                {% if results and results.columns[0][0] %}
                    {% set latest_ts = results.columns[0][0] %}

                    update {{ this }} as fct
                    set valid_to = '{{ latest_ts }}'
                    where valid_to = timestamp '9999-12-31'
                      and valid_from < '{{ latest_ts }}';
                {% endif %}
            {% endif %}
    """
    )
}}

with latest_fct_date as (
    select coalesce(max(valid_from), timestamp '1900-01-01') as max_valid_from
    from (
        {% if is_incremental() %}
            select valid_from from {{ this }}
        {% else %}
            select timestamp '1900-01-01' as valid_from
        {% endif %}
    )
),

offers as (
    select * from {{ ref('int_compute_offers') }}

    {% if is_incremental() %}
    where valid_from > (select max_valid_from from latest_fct_date)
    {% endif %}
),

exchange_rates as (
    select
        skey as exchange_rate_skey,
        value,
        valid_from,
        valid_to
    from {{ ref('dim_exchange_rates') }}
    where from_currency = 'USD'
      and to_currency   = 'MKD'
),

tariff_tiers as (
    select
        skey as tariff_tier_skey,
        consumer_category,
        tariff_type,
        metric,
        value as tariff_value,
        tariff_window_type,
        tariff_block_number,
        valid_from,
        valid_to
    from {{ ref('dim_electricity_tariff_tiers') }}
),

tariff_fees as (
    select
        consumer_category,
        max(case when fee_type = 'distribution' then value end) as distribution_fee,
        max(case when fee_type = 'access' then value end) as access_fee,
        valid_from,
        valid_to
    from {{ ref('dim_electricity_tariff_fees') }}
    group by consumer_category, valid_from, valid_to
),

tariff_blocks as (
    select
        consumer_category,
        tariff_window_type,
        tariff_block_number,
        lower_bound_kwh,
        upper_bound_kwh,
        valid_from,
        valid_to
    from {{ ref('dim_electricity_tariff_blocks') }}
),

joined as (
    select
        o.*,

        -- dim skeys
        er.exchange_rate_skey,
        tt.tariff_tier_skey,

        er.value as usd_to_mkd_rate,

        tt.consumer_category,
        tt.tariff_type,
        tt.metric as tariff_metric,
        tt.tariff_value,
        tt.tariff_window_type,
        tt.tariff_block_number,

        tf.distribution_fee,
        tf.access_fee,

        tb.lower_bound_kwh,
        tb.upper_bound_kwh

    from offers o

    join exchange_rates er
        on  cast(o.valid_from as date) >= er.valid_from
        and cast(o.valid_from as date) <  er.valid_to

    join tariff_tiers tt
        on  cast(o.valid_from as date) >= tt.valid_from
        and cast(o.valid_from as date) <  tt.valid_to

    join tariff_fees tf
        on  cast(o.valid_from as date) >= tf.valid_from
        and cast(o.valid_from as date) <  tf.valid_to
        and tt.consumer_category = tf.consumer_category

    left join tariff_blocks tb
        on  tt.consumer_category = tb.consumer_category
        and tt.tariff_window_type = tb.tariff_window_type
        and tt.tariff_block_number = tb.tariff_block_number
        and cast(o.valid_from as date) >= tb.valid_from
        and cast(o.valid_from as date) <  tb.valid_to
),

calculations as (
    select
        *,
        (gpu_tdp_watts * number_of_machine_gpus) / 1000.0        as total_system_kwh_per_hr,
        (gpu_tdp_watts * number_of_machine_gpus) / 1000.0
            / nullif(total_system_tflops, 0)                     as kwh_per_tflop,
        total_price_usd_per_hr                                   as revenue_usd_per_hr,
        total_price_usd_per_hr / nullif(number_of_machine_gpus, 0)
                                                                 as revenue_per_gpu_usd_per_hr
    from joined
),

with_vat_rate as (
    select
        *,
        {{ vat_rate('consumer_category') }} as vat_rate
    from calculations
),

-- Marginal cost is what running the GPU costs; average cost adds the access fee spread over the month (ADR-021).
cost_metrics as (
    select
        *,
        {{ cost_usd_per_hr('total_system_kwh_per_hr') }}                     as marginal_cost_usd_per_hr,
        {{ cost_usd_per_hr('total_system_kwh_per_hr', 'access_fee') }}       as average_cost_usd_per_hr
    from with_vat_rate
)

select
    -- identity / grain (one row per machine per census per tariff tier, ADR-020)
    machine_id,
    host_id,
    valid_from,
    valid_to,
    processed_at,

    -- dim skeys
    exchange_rate_skey,
    tariff_tier_skey,

    -- host context
    country_code,
    verification_flag,
    rentable_flag,
    reliability_score,

    -- gpu specs
    gpu_architecture,
    gpu_model_name,
    number_of_machine_gpus,
    tflops_per_gpu,
    gpu_tdp_watts,
    gpu_memory_gb,
    gpu_max_cuda_version_supported,
    gpu_bandwidth_gbytes_per_sec,

    -- cpu specs
    cpu_architecture,
    cpu_model_name,
    number_of_cpu_cores,
    cpu_clock_speed_ghz,

    -- system specs
    ram_gb,
    disk_model_name,
    disk_space_gb,
    disk_bandwidth_gbytes_per_sec,

    -- pcie
    pcie_generation,
    pcie_bandwidth_gbytes_per_sec,

    -- network
    network_download_mbits_per_sec,
    network_upload_mbits_per_sec,
    network_download_cost_usd_per_gbit,
    network_upload_cost_usd_per_gbit,

    -- performance scores
    deep_learning_score,
    deep_learning_score_per_usd,

    -- pricing
    gpu_price_usd_per_hr,
    minimum_bid_price_usd,
    storage_cost_usd_per_hr,
    revenue_usd_per_hr,
    revenue_per_gpu_usd_per_hr,

    -- derived power / compute
    total_system_kwh_per_hr,
    total_system_tflops,
    kwh_per_tflop,

    -- rates (locked at valid_from)
    usd_to_mkd_rate,

    -- tariff context
    consumer_category,
    tariff_type,
    tariff_metric,
    tariff_value,
    tariff_window_type,
    tariff_block_number,

    -- cost inputs
    distribution_fee,
    access_fee,
    vat_rate,

    -- costs (USD/hr)
    marginal_cost_usd_per_hr,
    average_cost_usd_per_hr,

    -- profits (USD/hr)
    revenue_usd_per_hr - marginal_cost_usd_per_hr as marginal_profit_usd_per_hr,
    revenue_usd_per_hr - average_cost_usd_per_hr  as average_profit_usd_per_hr,

    -- cost per TFLOP (USD)
    marginal_cost_usd_per_hr / nullif(total_system_tflops, 0) as marginal_cost_per_tflop_usd,
    average_cost_usd_per_hr  / nullif(total_system_tflops, 0) as average_cost_per_tflop_usd,

    -- profit per TFLOP (USD)
    (revenue_usd_per_hr - marginal_cost_usd_per_hr) / nullif(total_system_tflops, 0) as marginal_profit_per_tflop_usd,
    (revenue_usd_per_hr - average_cost_usd_per_hr)  / nullif(total_system_tflops, 0) as average_profit_per_tflop_usd

from cost_metrics
