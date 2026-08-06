{{
    config(
        materialized = 'table',
        tags         = ['marts'],
        cluster_by   = ['offer_type', 'gpu_model_name']
    )
}}

{% set consumer_category = var('consumer_category', 'household') %}
{% set kwh_consumed_so_far = var('kwh_consumed_so_far', 0) %}

with user_block as (
    select
        tariff_block_number,
        lower_bound_kwh,
        upper_bound_kwh
    from {{ ref('dim_electricity_tariff_blocks') }}
    where consumer_category = '{{ consumer_category }}'
      and tariff_window_type = 'high'
      and is_latest = true
      and {{ kwh_consumed_so_far }} >= lower_bound_kwh
      and ({{ kwh_consumed_so_far }} < upper_bound_kwh or upper_bound_kwh is null)
    limit 1
),

offers_in_block as (
    select
        f.*,
        ub.lower_bound_kwh,
        ub.upper_bound_kwh
    from {{ ref('fct_compute_offers') }} f
    join user_block ub
        on f.tariff_block_number = ub.tariff_block_number
    where f.consumer_category = '{{ consumer_category }}'
      and cast(f.valid_to as date) = date '9999-12-31'
      and coalesce(f.total_system_tflops, 0) > 0
      and coalesce(f.revenue_usd_per_hr, 0) > 0
      and f.rented_flag = false
      and f.rentable_flag = true
      and f.verification_flag = 'verified'
),

ranked as (
    select
        *,
        row_number() over (
            partition by offer_type, gpu_architecture, gpu_model_name, gpu_memory_gb
            order by profit_per_tflop_usd desc, reliability_score desc, valid_from desc
        ) as rn
    from offers_in_block
)

select
    offer_type,
    offer_id,
    machine_id,
    host_id,
    gpu_architecture,
    gpu_model_name,
    gpu_memory_gb,
    tflops_per_gpu,
    total_system_tflops,
    gpu_tdp_watts,
    gpu_bandwidth_gbytes_per_sec,
    gpu_max_cuda_version_supported,
    number_of_gpus,
    country_code,
    verification_flag,
    rentable_flag,
    rented_flag,
    reliability_score,
    kwh_per_tflop,
    gpu_price_usd_per_hr,
    revenue_usd_per_hr,
    cost_usd_per_hr,
    profit_usd_per_hr,
    cost_per_tflop_usd,
    profit_per_tflop_usd,
    tariff_tier_skey,
    consumer_category,
    tariff_window_type,
    tariff_block_number,
    lower_bound_kwh,
    upper_bound_kwh,
    valid_from,
    rn as profitability_rank

from ranked
where rn = 1
