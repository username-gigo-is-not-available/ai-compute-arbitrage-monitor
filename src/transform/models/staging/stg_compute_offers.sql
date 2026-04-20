{{
    config(
        materialized = 'incremental',
        unique_key   = ['offer_id', 'ingested_at'],
        tags = ['compute_offers']
    )
}}

with source as (
    select *
    from {{ source('sources', 'compute_offers') }}
),

renamed as (
    select
        -- ids
        cast(offer_id as int64)                                         as offer_id,
        cast(machine_id as int64)                                       as machine_id,
        cast(host_id as int64)                                          as host_id,

        -- type
        cast(offer_type as string)                                      as offer_type,

        -- prices
        cast(total_price_usd_per_hr as float64)                        as total_price_usd_per_hr,
        cast(gpu_price_usd_per_hr as float64)                          as gpu_price_usd_per_hr,
        cast(deep_learning_score_per_usd as float64)                   as deep_learning_score_per_usd,
        cast(minimum_bid_price_usd as float64)                         as minimum_bid_price_usd,
        cast(storage_cost_usd_per_hr as float64)                       as storage_cost_usd_per_hr,
        cast(network_upload_cost_usd_per_gbit as float64)              as network_upload_cost_usd_per_gbit,
        cast(network_download_cost_usd_per_gbit as float64)            as network_download_cost_usd_per_gbit,

        -- gpu
        cast(gpu_architecture as string)                               as gpu_architecture,
        cast(gpu_model_name as string)                                 as gpu_model_name,
        cast(gpu_memory_mb as int64)                                   as gpu_memory_mb,
        cast(gpu_tdp_watts as float64)                                 as gpu_tdp_watts,
        cast(number_of_gpus as int64)                                  as number_of_gpus,
        cast(gpu_max_cuda_version_supported as float64)                as gpu_max_cuda_version_supported,
        cast(gpu_tflops as float64)                                    as gpu_tflops,
        cast(gpu_bandwidth_gbytes_per_sec as float64)                  as gpu_bandwidth_gbytes_per_sec,

        -- cpu
        cast(cpu_architecture as string)                               as cpu_architecture,
        cast(cpu_model_name as string)                                 as cpu_model_name,
        cast(number_of_cpu_cores as int64)                             as number_of_cpu_cores,
        cast(cpu_clock_speed_ghz as float64)                           as cpu_clock_speed_ghz,

        -- ram / disk
        cast(ram_mb as int64)                                          as ram_mb,
        cast(disk_model_name as string)                                as disk_model_name,
        cast(disk_space_gb as float64)                                 as disk_space_gb,
        cast(disk_bandwidth_mbytes_per_sec as float64)                 as disk_bandwidth_mbytes_per_sec,

        -- pcie
        cast(pcie_generation as float64)                               as pcie_generation,
        cast(pcie_bandwidth_gbytes_per_sec as float64)                 as pcie_bandwidth_gbytes_per_sec,

        -- network
        cast(network_download_mbits_per_sec as float64)                as network_download_mbits_per_sec,
        cast(network_upload_mbits_per_sec as float64)                  as network_upload_mbits_per_sec,

        -- scores
        cast(reliability_score as float64)                             as reliability_score,
        cast(deep_learning_score as float64)                           as deep_learning_score,

        -- location
        cast(geolocation as string)                                    as geolocation,

        -- flags
        cast(verification_flag as string)                              as verification_flag,
        cast(rentable_flag as bool)                                    as rentable_flag,
        cast(rented_flag as bool)                                      as rented_flag,

        -- time
        {{ cast_utc('ingested_at') }}                                  as ingested_at,
        {{ cast_utc('processed_at') }}                                 as processed_at

    from source
)

select * from renamed

{% if is_incremental() %}
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}