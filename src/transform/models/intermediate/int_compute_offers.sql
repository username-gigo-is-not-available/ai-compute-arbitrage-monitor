{{
    config(
        tags = ['compute_offers']
    )
}}

with offers as (
    -- An offer rents number_of_offer_gpus of its machine's number_of_machine_gpus (rented or not, ADR-020).
    -- Prices per GPU are the same on every offer of a machine (assert_stg_compute_offers_machine_slices_agree).
    select
        *,
        {{ number_of_machine_gpus() }}                                         as number_of_machine_gpus,
        gpu_price_usd_per_hr / number_of_offer_gpus                            as base_price_per_gpu_usd_per_hr,
        total_price_usd_per_hr / number_of_offer_gpus                          as total_price_per_gpu_usd_per_hr,
        minimum_bid_price_usd / number_of_offer_gpus                           as minimum_bid_price_per_gpu_usd,
        gpu_tflops / number_of_offer_gpus                                      as tflops_per_gpu
    from {{ ref('stg_compute_offers') }}
),

machines as (
    -- One row per machine: its largest offer carries the specs. The window columns see all of its offers,
    -- because qualify filters after they are computed.
    select
        *,
        logical_or(rentable_flag) over machine_census                          as any_offer_rentable,
        min(minimum_bid_price_per_gpu_usd) over machine_census                 as lowest_minimum_bid_price_per_gpu_usd
    from offers
    qualify row_number() over (partition by machine_id, snapshot_at order by number_of_offer_gpus desc, offer_id) = 1
    window machine_census as (partition by machine_id, snapshot_at)
),

transformed as (
    select
        -- ids
        machine_id,
        host_id,

        -- prices for the whole machine: per GPU x GPUs in the machine
        total_price_per_gpu_usd_per_hr * number_of_machine_gpus                as total_price_usd_per_hr,
        base_price_per_gpu_usd_per_hr * number_of_machine_gpus                 as gpu_price_usd_per_hr,
        deep_learning_score_per_usd,
        lowest_minimum_bid_price_per_gpu_usd * number_of_machine_gpus          as minimum_bid_price_usd,
        storage_cost_usd_per_hr,
        network_upload_cost_usd_per_gbit,
        network_download_cost_usd_per_gbit,

        -- gpu
        gpu_architecture,
        gpu_model_name,
        cast({{ mb_to_gb(round_gpu_mb('gpu_memory_mb')) }} as int64)           as gpu_memory_gb,
        gpu_tdp_watts,
        number_of_machine_gpus,
        round(gpu_max_cuda_version_supported, 1)                               as gpu_max_cuda_version_supported,
        tflops_per_gpu,
        tflops_per_gpu * number_of_machine_gpus                                as total_system_tflops,
        gpu_bandwidth_gbytes_per_sec,

        -- cpu (as allocated to the largest offer)
        cpu_architecture,
        cpu_model_name,
        number_of_cpu_cores,
        cpu_clock_speed_ghz,

        -- ram / disk (as allocated to the largest offer)
        {{ mb_to_gb('ram_mb') }}                                               as ram_gb,
        disk_model_name,
        disk_space_gb,
        {{ mb_to_gb('disk_bandwidth_mbytes_per_sec') }}                        as disk_bandwidth_gbytes_per_sec,

        -- pcie
        pcie_generation,
        pcie_bandwidth_gbytes_per_sec,

        -- network
        network_download_mbits_per_sec,
        network_upload_mbits_per_sec,

        -- scores (of the largest offer)
        reliability_score,
        deep_learning_score,

        -- location
        {{ extract_country_code('geolocation') }}                              as country_code,

        -- flags: available when any of its offers can be rented now; otherwise taken (CONTEXT.md)
        verification_flag,
        any_offer_rentable                                                     as rentable_flag,

        -- time
        snapshot_at                                                            as valid_from,
        cast('9999-12-31' as timestamp)                                        as valid_to,
        processed_at

    from machines
)

select * from transformed
