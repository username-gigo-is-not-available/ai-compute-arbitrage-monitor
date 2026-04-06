with current_offers as (
    select * from {{ ref('fct_compute_offers') }}
    where cast(valid_to as date) = '9999-12-31'
),

available_offers as (
    select * from current_offers
    where
        coalesce(total_system_tflops, 0) > 0
        and coalesce(revenue_usd_per_hr, 0) > 0
        and coalesce(gpu_tdp_watts, 0) > 0
        and rented_flag = false
        and rentable_flag = true
        and verification_flag = 'verified'
),

normalized_offers as (
    select
        *,
        row_number() over (
            partition by offer_type, gpu_architecture, gpu_model_name, gpu_memory_gb
            order by valid_from desc, (profit_business_high_usd_per_hr / nullif(number_of_gpus, 0)) desc
        ) as rn
    from available_offers
)

select
    offer_type,
    gpu_architecture,
    gpu_model_name,
    gpu_memory_gb,
    gpu_tdp_watts,
    tflops_per_gpu,
    kwh_per_tflop,
    verification_flag,
    rentable_flag,
    rented_flag,
    reliability_score,
    country_code,

    revenue_usd_per_hr / nullif(number_of_gpus, 0)                  as revenue_per_gpu_usd_per_hr,

    cost_household_1_high_usd_per_hr / nullif(number_of_gpus, 0)    as cost_household_1_high_per_gpu_usd_per_hr,
    cost_household_2_high_usd_per_hr / nullif(number_of_gpus, 0)    as cost_household_2_high_per_gpu_usd_per_hr,
    cost_household_3_high_usd_per_hr / nullif(number_of_gpus, 0)    as cost_household_3_high_per_gpu_usd_per_hr,
    cost_household_4_high_usd_per_hr / nullif(number_of_gpus, 0)    as cost_household_4_high_per_gpu_usd_per_hr,
    cost_household_low_usd_per_hr    / nullif(number_of_gpus, 0)    as cost_household_low_per_gpu_usd_per_hr,
    cost_business_high_usd_per_hr    / nullif(number_of_gpus, 0)    as cost_business_high_per_gpu_usd_per_hr,
    cost_business_low_usd_per_hr     / nullif(number_of_gpus, 0)    as cost_business_low_per_gpu_usd_per_hr,

    profit_household_1_high_usd_per_hr / nullif(number_of_gpus, 0)  as profit_household_1_high_per_gpu_usd_per_hr,
    profit_household_2_high_usd_per_hr / nullif(number_of_gpus, 0)  as profit_household_2_high_per_gpu_usd_per_hr,
    profit_household_3_high_usd_per_hr / nullif(number_of_gpus, 0)  as profit_household_3_high_per_gpu_usd_per_hr,
    profit_household_4_high_usd_per_hr / nullif(number_of_gpus, 0)  as profit_household_4_high_per_gpu_usd_per_hr,
    profit_household_low_usd_per_hr    / nullif(number_of_gpus, 0)  as profit_household_low_per_gpu_usd_per_hr,
    profit_business_high_usd_per_hr    / nullif(number_of_gpus, 0)  as profit_business_high_per_gpu_usd_per_hr,
    profit_business_low_usd_per_hr     / nullif(number_of_gpus, 0)  as profit_business_low_per_gpu_usd_per_hr,

    profit_per_tflop_household_1_high_usd,
    profit_per_tflop_household_2_high_usd,
    profit_per_tflop_household_3_high_usd,
    profit_per_tflop_household_4_high_usd,
    profit_per_tflop_household_low_usd,
    profit_per_tflop_business_high_usd,
    profit_per_tflop_business_low_usd,

    valid_from as ingested_at
from normalized_offers
where rn = 1
order by offer_type, gpu_architecture, gpu_model_name