{{
    config(
        materialized     = 'incremental',
        unique_key       = ['offer_id', 'valid_from'],
        on_schema_change = 'append_new_columns',
        post_hook = """
            {% if execute %}
                {% set latest_ts_query %}
                    select max(valid_from) from {{ this }}
                {% endset %}

                {% set results = run_query(latest_ts_query) %}

                {% if results and results.columns[0][0] %}
                    {% set latest_ts = results.columns[0][0] %}

                    update {{ this }}
                    set valid_to = '{{ latest_ts }}'
                    where valid_to = timezone('UTC', '9999-12-31'::timestamp)
                      and valid_from < '{{ latest_ts }}'
                      and offer_id not in (
                          select offer_id
                          from {{ ref('int_compute_offers') }}
                          where valid_from = '{{ latest_ts }}'
                      );
                {% endif %}
            {% endif %}
        """
    )
}}
with latest_fct_date as (
    select coalesce(max(valid_from), '1900-01-01'::timestamp) as max_valid_from
    from {{ this }}
),

offers as (
    select * from {{ ref('int_compute_offers') }}

    {% if is_incremental() %}
    where valid_from > (select max_valid_from from latest_fct_date)
    {% endif %}
),

exchange_rates as (
    select
        skey,
        value,
        valid_from,
        valid_to
    from {{ ref('dim_exchange_rates') }}
    where from_currency = 'USD'
      and to_currency   = 'MKD'
),

tariff_prices_pivoted as (
    select
        valid_from,
        valid_to,
        max(case when tariff_description_en = 'household_high_tariff_block_1' then price_mkd_per_kwh end) as household_high_tariff_block_1,
        max(case when tariff_description_en = 'household_high_tariff_block_2' then price_mkd_per_kwh end) as household_high_tariff_block_2,
        max(case when tariff_description_en = 'household_high_tariff_block_3' then price_mkd_per_kwh end) as household_high_tariff_block_3,
        max(case when tariff_description_en = 'household_high_tariff_block_4' then price_mkd_per_kwh end) as household_high_tariff_block_4,
        max(case when tariff_description_en = 'household_low_tariff_block'    then price_mkd_per_kwh end) as household_low_tariff_block,
        max(case when tariff_description_en = 'business_high_tariff_block'    then price_mkd_per_kwh end) as business_high_tariff_block,
        max(case when tariff_description_en = 'business_low_tariff_block'     then price_mkd_per_kwh end) as business_low_tariff_block,
        max(case when tariff_description_en = 'distribution_fee'              then price_mkd_per_kwh end) as distribution_fee
    from {{ ref('dim_electricity_tariff_prices') }}
    group by valid_from, valid_to
),

joined as (
    select
        o.*,

        -- dim skeys
        er.skey as exchange_rate_skey,

        er.value as usd_to_mkd_rate,

        tp.household_high_tariff_block_1,
        tp.household_high_tariff_block_2,
        tp.household_high_tariff_block_3,
        tp.household_high_tariff_block_4,
        tp.household_low_tariff_block,
        tp.business_high_tariff_block,
        tp.business_low_tariff_block,
        tp.distribution_fee

    from offers o

    left join exchange_rates er
        on  o.valid_from >= er.valid_from
        and o.valid_from <  er.valid_to

    left join tariff_prices_pivoted tp
        on  o.valid_from >= tp.valid_from
        and o.valid_from <  tp.valid_to
),

calculations as (
    select
        *,
        (gpu_tdp_watts * number_of_gpus) / 1000.0                as total_system_kwh_per_hr,
        (gpu_tdp_watts * number_of_gpus) / 1000.0
            / nullif(total_system_tflops, 0)                     as kwh_per_tflop,
        total_price_usd_per_hr                                   as revenue_usd_per_hr
    from joined
),

cost_metrics as (
    select
        *,
        (total_system_kwh_per_hr * (household_high_tariff_block_1 + distribution_fee)) / nullif(usd_to_mkd_rate, 0) as cost_household_1_high_usd_per_hr,
        (total_system_kwh_per_hr * (household_high_tariff_block_2 + distribution_fee)) / nullif(usd_to_mkd_rate, 0) as cost_household_2_high_usd_per_hr,
        (total_system_kwh_per_hr * (household_high_tariff_block_3 + distribution_fee)) / nullif(usd_to_mkd_rate, 0) as cost_household_3_high_usd_per_hr,
        (total_system_kwh_per_hr * (household_high_tariff_block_4 + distribution_fee)) / nullif(usd_to_mkd_rate, 0) as cost_household_4_high_usd_per_hr,
        (total_system_kwh_per_hr * (household_low_tariff_block    + distribution_fee)) / nullif(usd_to_mkd_rate, 0) as cost_household_low_usd_per_hr,
        (total_system_kwh_per_hr * (business_high_tariff_block    + distribution_fee)) / nullif(usd_to_mkd_rate, 0) as cost_business_high_usd_per_hr,
        (total_system_kwh_per_hr * (business_low_tariff_block     + distribution_fee)) / nullif(usd_to_mkd_rate, 0) as cost_business_low_usd_per_hr
    from calculations
)

select
    -- -------------------------------------------------------------------------
    -- identity / grain
    -- -------------------------------------------------------------------------
    offer_id,
    machine_id,
    host_id,
    valid_from,
    valid_to,
    processed_at,

    -- -------------------------------------------------------------------------
    -- offer type
    -- -------------------------------------------------------------------------
    offer_type,

    -- -------------------------------------------------------------------------
    -- foreign keys to dims (skeys)
    -- -------------------------------------------------------------------------
    exchange_rate_skey,

    -- -------------------------------------------------------------------------
    -- host context
    -- -------------------------------------------------------------------------
    country_code,
    verification_flag,
    rentable_flag,
    rented_flag,
    reliability_score,

    -- -------------------------------------------------------------------------
    -- gpu specs
    -- -------------------------------------------------------------------------
    gpu_architecture,
    gpu_model_name,
    number_of_gpus,
    tflops_per_gpu,
    gpu_tdp_watts,
    gpu_memory_gb,
    gpu_max_cuda_version_supported,
    gpu_bandwidth_gbytes_per_sec,

    -- -------------------------------------------------------------------------
    -- cpu specs
    -- -------------------------------------------------------------------------
    cpu_architecture,
    cpu_model_name,
    number_of_cpu_cores,
    cpu_clock_speed_ghz,

    -- -------------------------------------------------------------------------
    -- system specs
    -- -------------------------------------------------------------------------
    ram_gb,
    disk_model_name,
    disk_space_gb,
    disk_bandwidth_gbytes_per_sec,

    -- -------------------------------------------------------------------------
    -- pcie
    -- -------------------------------------------------------------------------
    pcie_generation,
    pcie_bandwidth_gbytes_per_sec,

    -- -------------------------------------------------------------------------
    -- network
    -- -------------------------------------------------------------------------
    network_download_mbits_per_sec,
    network_upload_mbits_per_sec,
    network_download_cost_usd_per_gbit,
    network_upload_cost_usd_per_gbit,

    -- -------------------------------------------------------------------------
    -- performance scores
    -- -------------------------------------------------------------------------
    deep_learning_score,
    deep_learning_score_per_usd,

    -- -------------------------------------------------------------------------
    -- pricing
    -- -------------------------------------------------------------------------
    gpu_price_usd_per_hr,
    minimum_bid_price_usd,
    storage_cost_usd_per_hr,
    revenue_usd_per_hr,

    -- -------------------------------------------------------------------------
    -- derived power / compute
    -- -------------------------------------------------------------------------
    total_system_kwh_per_hr,
    total_system_tflops,
    kwh_per_tflop,

    -- -------------------------------------------------------------------------
    -- rates context (locked at valid_from)
    -- -------------------------------------------------------------------------
    usd_to_mkd_rate,

    -- -------------------------------------------------------------------------
    -- costs (USD/hr)
    -- -------------------------------------------------------------------------
    cost_household_1_high_usd_per_hr,
    cost_household_2_high_usd_per_hr,
    cost_household_3_high_usd_per_hr,
    cost_household_4_high_usd_per_hr,
    cost_household_low_usd_per_hr,
    cost_business_high_usd_per_hr,
    cost_business_low_usd_per_hr,

    -- -------------------------------------------------------------------------
    -- profits (USD/hr)
    -- -------------------------------------------------------------------------
    revenue_usd_per_hr - cost_household_1_high_usd_per_hr as profit_household_1_high_usd_per_hr,
    revenue_usd_per_hr - cost_household_2_high_usd_per_hr as profit_household_2_high_usd_per_hr,
    revenue_usd_per_hr - cost_household_3_high_usd_per_hr as profit_household_3_high_usd_per_hr,
    revenue_usd_per_hr - cost_household_4_high_usd_per_hr as profit_household_4_high_usd_per_hr,
    revenue_usd_per_hr - cost_household_low_usd_per_hr    as profit_household_low_usd_per_hr,
    revenue_usd_per_hr - cost_business_high_usd_per_hr    as profit_business_high_usd_per_hr,
    revenue_usd_per_hr - cost_business_low_usd_per_hr     as profit_business_low_usd_per_hr,

    -- -------------------------------------------------------------------------
    -- cost per TFLOP (USD)
    -- -------------------------------------------------------------------------
    cost_household_1_high_usd_per_hr / nullif(total_system_tflops, 0) as cost_per_tflop_household_1_high_usd,
    cost_household_2_high_usd_per_hr / nullif(total_system_tflops, 0) as cost_per_tflop_household_2_high_usd,
    cost_household_3_high_usd_per_hr / nullif(total_system_tflops, 0) as cost_per_tflop_household_3_high_usd,
    cost_household_4_high_usd_per_hr / nullif(total_system_tflops, 0) as cost_per_tflop_household_4_high_usd,
    cost_household_low_usd_per_hr    / nullif(total_system_tflops, 0) as cost_per_tflop_household_low_usd,
    cost_business_high_usd_per_hr    / nullif(total_system_tflops, 0) as cost_per_tflop_business_high_usd,
    cost_business_low_usd_per_hr     / nullif(total_system_tflops, 0) as cost_per_tflop_business_low_usd,

    -- -------------------------------------------------------------------------
    -- profit per TFLOP (USD)
    -- -------------------------------------------------------------------------
    (revenue_usd_per_hr - cost_household_1_high_usd_per_hr) / nullif(total_system_tflops, 0) as profit_per_tflop_household_1_high_usd,
    (revenue_usd_per_hr - cost_household_2_high_usd_per_hr) / nullif(total_system_tflops, 0) as profit_per_tflop_household_2_high_usd,
    (revenue_usd_per_hr - cost_household_3_high_usd_per_hr) / nullif(total_system_tflops, 0) as profit_per_tflop_household_3_high_usd,
    (revenue_usd_per_hr - cost_household_4_high_usd_per_hr) / nullif(total_system_tflops, 0) as profit_per_tflop_household_4_high_usd,
    (revenue_usd_per_hr - cost_household_low_usd_per_hr)    / nullif(total_system_tflops, 0) as profit_per_tflop_household_low_usd,
    (revenue_usd_per_hr - cost_business_high_usd_per_hr)    / nullif(total_system_tflops, 0) as profit_per_tflop_business_high_usd,
    (revenue_usd_per_hr - cost_business_low_usd_per_hr)     / nullif(total_system_tflops, 0) as profit_per_tflop_business_low_usd

from cost_metrics
