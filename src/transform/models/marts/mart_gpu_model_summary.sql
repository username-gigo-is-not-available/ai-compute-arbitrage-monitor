select
    offer_type,
    gpu_architecture,
    gpu_model_name,
    tflops_per_gpu,
    gpu_memory_gb,
    count(*)                                    as total_offer_observations,

    avg(kwh_per_tflop)                          as avg_kwh_per_tflop,

    avg(revenue_usd_per_hr / nullif(number_of_gpus, 0))                 as avg_revenue_per_gpu_usd_per_hr,

    round(100.0 * sum(case when profit_household_1_high_usd_per_hr > 0 then 1 else 0 end)
        / nullif(count(*), 0), 2)               as pct_profitable_household_1_high,
    round(100.0 * sum(case when profit_household_2_high_usd_per_hr > 0 then 1 else 0 end)
        / nullif(count(*), 0), 2)               as pct_profitable_household_2_high,
    round(100.0 * sum(case when profit_household_3_high_usd_per_hr > 0 then 1 else 0 end)
        / nullif(count(*), 0), 2)               as pct_profitable_household_3_high,
    round(100.0 * sum(case when profit_household_4_high_usd_per_hr > 0 then 1 else 0 end)
        / nullif(count(*), 0), 2)               as pct_profitable_household_4_high,
    round(100.0 * sum(case when profit_business_high_usd_per_hr > 0 then 1 else 0 end)
        / nullif(count(*), 0), 2)               as pct_profitable_business_high,
    round(100.0 * sum(case when profit_business_low_usd_per_hr > 0 then 1 else 0 end)
        / nullif(count(*), 0), 2)               as pct_profitable_business_low,

    round(100.0 * sum(case when verification_flag = 'verified' then 1 else 0 end)
        / nullif(count(*), 0), 2)               as pct_verified,
    round(100.0 * sum(case when rented_flag = false then 1 else 0 end)
        / nullif(count(*), 0), 2)               as pct_available,
    avg(reliability_score)                      as avg_reliability_score,

    max(valid_from)                             as last_seen_at

from {{ ref('fct_compute_offers') }}
where cast(valid_to as date) = '9999-12-31'
group by 1, 2, 3, 4, 5
order by pct_profitable_business_high desc