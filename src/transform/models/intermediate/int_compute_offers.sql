{{
    config(
        tags = ['compute_offers']
    )
}}

with source as (
    select *
    from {{ ref('stg_compute_offers') }}
),

transformed as (
    select
        -- ids
        offer_id,
        machine_id,
        host_id,

        -- type
        offer_type,

        -- prices
        total_price_usd_per_hr,
        gpu_price_usd_per_hr,
        deep_learning_score_per_usd,
        minimum_bid_price_usd,
        storage_cost_usd_per_hr,
        network_upload_cost_usd_per_gbit,
        network_download_cost_usd_per_gbit,

        -- gpu
        gpu_architecture,
        gpu_model_name,
        cast({{ mb_to_gb('cast(' ~ round_gpu_mb('gpu_memory_mb') ~ ' as int64)') }} as int64) as gpu_memory_gb,
        gpu_tdp_watts,
        number_of_gpus,
        round(gpu_max_cuda_version_supported, 1)                as gpu_max_cuda_version_supported,
        (gpu_tflops / number_of_gpus)                           as tflops_per_gpu,
        gpu_tflops                                              as total_system_tflops,
        gpu_bandwidth_gbytes_per_sec,

        -- cpu
        cpu_architecture,
        cpu_model_name,
        number_of_cpu_cores,
        cpu_clock_speed_ghz,

        -- ram / disk
        {{ mb_to_gb('ram_mb') }}                                as ram_gb,
        disk_model_name,
        disk_space_gb,
        {{ mb_to_gb('disk_bandwidth_mbytes_per_sec') }}         as disk_bandwidth_gbytes_per_sec,

        -- pcie
        pcie_generation,
        pcie_bandwidth_gbytes_per_sec,

        -- network
        network_download_mbits_per_sec,
        network_upload_mbits_per_sec,

        -- scores
        reliability_score,
        deep_learning_score,

        -- location
        {{ extract_country_code('geolocation') }}               as country_code,

        -- flags
        verification_flag,
        rentable_flag,
        rented_flag,

        -- time
        ingested_at                                             as valid_from,
        cast('9999-12-31' as timestamp)                         as valid_to,
        processed_at

    from source
)

select * from transformed