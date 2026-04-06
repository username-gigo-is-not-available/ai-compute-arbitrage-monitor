{{
    config(
        materialized = 'incremental',
        unique_key   = ['offer_id', 'ingested_at']
    )
}}

with source as (
    select *
    from read_parquet('C:\\Users\\grigo\\PycharmProjects\\gpu_ai_compute_arbitrage_monitor\\data\\silver\\sources\\compute_offers\\*.parquet')
),

renamed as (
    select
        -- ids
        cast(offer_id as integer)                                       as offer_id,
        cast(machine_id as integer)                                     as machine_id,
        cast(host_id as integer)                                        as host_id,

        -- type
        cast(offer_type as varchar)                                     as offer_type,

        -- prices
        cast(total_price_usd_per_hr as double)                          as total_price_usd_per_hr,
        cast(gpu_price_usd_per_hr as double)                            as gpu_price_usd_per_hr,
        cast(deep_learning_score_per_usd as double)                     as deep_learning_score_per_usd,
        cast(minimum_bid_price_usd as double)                           as minimum_bid_price_usd,
        cast(storage_cost_usd_per_hr as double)                         as storage_cost_usd_per_hr,
        cast(network_upload_cost_usd_per_gbit as double)                as network_upload_cost_usd_per_gbit,
        cast(network_download_cost_usd_per_gbit as double)              as network_download_cost_usd_per_gbit,

        -- gpu
        cast(gpu_architecture as varchar)                               as gpu_architecture,
        cast(gpu_model_name as varchar)                                 as gpu_model_name,
        cast(gpu_memory_mb as integer)                                  as gpu_memory_mb,
        cast(gpu_tdp_watts as double)                                   as gpu_tdp_watts,
        cast(number_of_gpus as integer)                                 as number_of_gpus,
        cast(gpu_max_cuda_version_supported as double)                  as gpu_max_cuda_version_supported,
        cast(gpu_tflops as double)                                      as gpu_tflops,
        cast(gpu_bandwidth_gbytes_per_sec as double)                    as gpu_bandwidth_gbytes_per_sec,

        -- cpu
        cast(cpu_architecture as varchar)                               as cpu_architecture,
        cast(cpu_model_name as varchar)                                 as cpu_model_name,
        cast(number_of_cpu_cores as integer)                            as number_of_cpu_cores,
        cast(cpu_clock_speed_ghz as double)                             as cpu_clock_speed_ghz,

        -- ram / disk
        cast(ram_mb as integer)                                         as ram_mb,
        cast(disk_model_name as varchar)                                as disk_model_name,
        cast(disk_space_gb as double)                                   as disk_space_gb,
        cast(disk_bandwidth_mbytes_per_sec as double)                   as disk_bandwidth_mbytes_per_sec,

        -- pcie
        cast(pcie_generation as double)                                 as pcie_generation,
        cast(pcie_bandwidth_gbytes_per_sec as double)                   as pcie_bandwidth_gbytes_per_sec,

        -- network
        cast(network_download_mbits_per_sec as double)                  as network_download_mbits_per_sec,
        cast(network_upload_mbits_per_sec as double)                    as network_upload_mbits_per_sec,

        -- scores
        cast(reliability_score as double)                               as reliability_score,
        cast(deep_learning_score as double)                             as deep_learning_score,

        -- location
        cast(geolocation as varchar)                                    as geolocation,

        -- flags
        cast(verification_flag as varchar)                              as verification_flag,
        cast(rentable_flag as boolean)                                  as rentable_flag,
        cast(rented_flag as boolean)                                    as rented_flag,

        -- time
        {{ cast_utc('ingested_at') }}                                   as ingested_at,
        {{ cast_utc('processed_at') }}                                  as processed_at

    from source
)

select * from renamed

{% if is_incremental() %}
  where ingested_at > (select max(ingested_at) from {{ this }})
{% endif %}
