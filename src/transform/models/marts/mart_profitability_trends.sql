select
    date_trunc('hour', f.valid_from)            as hour_bucket,
    extract(isodow from f.valid_from)           as day_of_week,
    extract(hour from f.valid_from)             as hour_of_day,
    ts.tariff_type                              as tariff_window,
    f.offer_type,
    f.gpu_architecture,
    f.gpu_model_name,
    f.gpu_memory_gb,
    f.gpu_tdp_watts,
    f.gpu_bandwidth_gbytes_per_sec,
    f.gpu_max_cuda_version_supported,
    f.number_of_gpus,
    count(*)                                    as offer_count,

    avg(f.revenue_usd_per_hr)                   as avg_revenue_usd_per_hr,

    avg(f.profit_household_1_high_usd_per_hr)   as avg_profit_household_1_high_usd_per_hr,
    avg(f.profit_household_2_high_usd_per_hr)   as avg_profit_household_2_high_usd_per_hr,
    avg(f.profit_household_3_high_usd_per_hr)   as avg_profit_household_3_high_usd_per_hr,
    avg(f.profit_household_4_high_usd_per_hr)   as avg_profit_household_4_high_usd_per_hr,
    avg(f.profit_household_low_usd_per_hr)      as avg_profit_household_low_usd_per_hr,
    avg(f.profit_business_high_usd_per_hr)      as avg_profit_business_high_usd_per_hr,
    avg(f.profit_business_low_usd_per_hr)       as avg_profit_business_low_usd_per_hr,

    min(f.profit_household_1_high_usd_per_hr)   as min_profit_household_1_high_usd_per_hr,
    min(f.profit_household_2_high_usd_per_hr)   as min_profit_household_2_high_usd_per_hr,
    min(f.profit_household_3_high_usd_per_hr)   as min_profit_household_3_high_usd_per_hr,
    min(f.profit_household_4_high_usd_per_hr)   as min_profit_household_4_high_usd_per_hr,
    min(f.profit_household_low_usd_per_hr)      as min_profit_household_low_usd_per_hr,
    min(f.profit_business_high_usd_per_hr)      as min_profit_business_high_usd_per_hr,
    min(f.profit_business_low_usd_per_hr)       as min_profit_business_low_usd_per_hr,

    max(f.profit_household_1_high_usd_per_hr)   as max_profit_household_1_high_usd_per_hr,
    max(f.profit_household_2_high_usd_per_hr)   as max_profit_household_2_high_usd_per_hr,
    max(f.profit_household_3_high_usd_per_hr)   as max_profit_household_3_high_usd_per_hr,
    max(f.profit_household_4_high_usd_per_hr)   as max_profit_household_4_high_usd_per_hr,
    max(f.profit_household_low_usd_per_hr)      as max_profit_household_low_usd_per_hr,
    max(f.profit_business_high_usd_per_hr)      as max_profit_business_high_usd_per_hr,
    max(f.profit_business_low_usd_per_hr)       as max_profit_business_low_usd_per_hr,

    avg(f.profit_per_tflop_household_1_high_usd)   as avg_profit_per_tflop_household_1_high_usd,
    avg(f.profit_per_tflop_household_2_high_usd)   as avg_profit_per_tflop_household_2_high_usd,
    avg(f.profit_per_tflop_household_3_high_usd)   as avg_profit_per_tflop_household_3_high_usd,
    avg(f.profit_per_tflop_household_4_high_usd)   as avg_profit_per_tflop_household_4_high_usd,
    avg(f.profit_per_tflop_household_low_usd)      as avg_profit_per_tflop_household_low_usd,
    avg(f.profit_per_tflop_business_high_usd)      as avg_profit_per_tflop_business_high_usd,
    avg(f.profit_per_tflop_business_low_usd)       as avg_profit_per_tflop_business_low_usd,

    min(f.profit_per_tflop_household_1_high_usd)   as min_profit_per_tflop_household_1_high_usd,
    min(f.profit_per_tflop_household_2_high_usd)   as min_profit_per_tflop_household_2_high_usd,
    min(f.profit_per_tflop_household_3_high_usd)   as min_profit_per_tflop_household_3_high_usd,
    min(f.profit_per_tflop_household_4_high_usd)   as min_profit_per_tflop_household_4_high_usd,
    min(f.profit_per_tflop_household_low_usd)      as min_profit_per_tflop_household_low_usd,
    min(f.profit_per_tflop_business_high_usd)      as min_profit_per_tflop_business_high_usd,
    min(f.profit_per_tflop_business_low_usd)       as min_profit_per_tflop_business_low_usd,

    max(f.profit_per_tflop_household_1_high_usd)   as max_profit_per_tflop_household_1_high_usd,
    max(f.profit_per_tflop_household_2_high_usd)   as max_profit_per_tflop_household_2_high_usd,
    max(f.profit_per_tflop_household_3_high_usd)   as max_profit_per_tflop_household_3_high_usd,
    max(f.profit_per_tflop_household_4_high_usd)   as max_profit_per_tflop_household_4_high_usd,
    max(f.profit_per_tflop_household_low_usd)      as max_profit_per_tflop_household_low_usd,
    max(f.profit_per_tflop_business_high_usd)      as max_profit_per_tflop_business_high_usd,
    max(f.profit_per_tflop_business_low_usd)       as max_profit_per_tflop_business_low_usd

from {{ ref('fct_compute_offers') }} f
left join {{ ref('dim_electricity_tariffs_schedule') }} ts
    on  extract(isodow from f.valid_from) = ts.day_of_week
    and extract(hour from f.valid_from)   = ts.hour
    and f.valid_from >= ts.valid_from
    and (f.valid_from < ts.valid_to)
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12
order by 1 desc, 2, 3, 4
