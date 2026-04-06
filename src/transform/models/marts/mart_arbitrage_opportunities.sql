with current_offers as (
    select * from {{ ref('fct_compute_offers') }}
    where cast(valid_to as date) = '9999-12-31'
),

available_offers as (
    select * from current_offers
    where
        coalesce(total_system_tflops, 0) > 0
        and coalesce(revenue_usd_per_hr, 0) > 0
      and coalesce(gpu_tdp_watts, 0) <> 0
        and rented_flag = false
        and rentable_flag = true
        and verification_flag = 'verified'
),

ranked as (
    select
        *,
        row_number() over (
            partition by offer_type
            order by
                profit_per_tflop_business_high_usd desc,
                reliability_score desc,
                gpu_tdp_watts asc,
                pcie_bandwidth_gbytes_per_sec desc,
                gpu_bandwidth_gbytes_per_sec desc,
                valid_from desc
        ) as arbitrage_rank
    from available_offers
)

select
    arbitrage_rank,
    offer_type,
    offer_id,
    machine_id,
    host_id,

    -- location / host
    country_code,
    reliability_score,

    -- gpu
    gpu_architecture,
    gpu_model_name,
    number_of_gpus,
    tflops_per_gpu,
    total_system_tflops,
    gpu_memory_gb,
    gpu_tdp_watts,
    gpu_bandwidth_gbytes_per_sec,
    gpu_max_cuda_version_supported,

    -- cpu
    cpu_architecture,
    cpu_model_name,
    number_of_cpu_cores,
    cpu_clock_speed_ghz,

    -- system
    ram_gb,
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

    -- efficiency
    kwh_per_tflop,
    deep_learning_score,
    deep_learning_score_per_usd,

    -- pricing
    gpu_price_usd_per_hr,
    minimum_bid_price_usd,
    storage_cost_usd_per_hr,
    revenue_usd_per_hr,

    -- rates
    usd_to_mkd_rate,

    -- profits (USD/hr)
    profit_household_1_high_usd_per_hr,
    profit_household_2_high_usd_per_hr,
    profit_household_3_high_usd_per_hr,
    profit_household_4_high_usd_per_hr,
    profit_household_low_usd_per_hr,
    profit_business_high_usd_per_hr,
    profit_business_low_usd_per_hr,

    -- profit per TFLOP (USD)
    profit_per_tflop_household_1_high_usd,
    profit_per_tflop_household_2_high_usd,
    profit_per_tflop_household_3_high_usd,
    profit_per_tflop_household_4_high_usd,
    profit_per_tflop_household_low_usd,
    profit_per_tflop_business_high_usd,
    profit_per_tflop_business_low_usd,

    valid_from

from ranked
order by offer_type, arbitrage_rank