{{
    config(
        tags = ['compute_offers']
    )
}}

with census_offers as (
    select
        *,
        cast(round(number_of_gpus / gpu_fraction_of_machine) as int64)         as machine_gpus
    from {{ ref('stg_compute_offers') }}
    -- Gold starts at the first census: earlier snapshots were random samples across three offer types,
    -- without the machine size needed to collapse them (ADR-020).
    where gpu_fraction_of_machine is not null
      and offer_type = 'on_demand'
),

machines as (
    -- A machine is listed as several overlapping offers (slices) that share its per-GPU price and size
    -- (assert_stg_compute_offers_machine_slices_agree). Its largest offer carries the specs; the window
    -- columns see all of its offers, because qualify filters after they are computed.
    select
        *,
        logical_or(rentable_flag) over machine_census                          as any_offer_rentable,
        min(minimum_bid_price_usd / number_of_gpus) over machine_census        as minimum_bid_price_usd_per_gpu
    from census_offers
    qualify row_number() over (partition by machine_id, snapshot_at order by number_of_gpus desc, offer_id) = 1
    window machine_census as (partition by machine_id, snapshot_at)
),

transformed as (
    select
        -- ids
        machine_id,
        host_id,

        -- prices: per GPU (identical across a machine's offers) x machine size
        total_price_usd_per_hr / number_of_gpus * machine_gpus                 as total_price_usd_per_hr,
        gpu_price_usd_per_hr / number_of_gpus * machine_gpus                   as gpu_price_usd_per_hr,
        deep_learning_score_per_usd,
        minimum_bid_price_usd_per_gpu * machine_gpus                           as minimum_bid_price_usd,
        storage_cost_usd_per_hr,
        network_upload_cost_usd_per_gbit,
        network_download_cost_usd_per_gbit,

        -- gpu
        gpu_architecture,
        gpu_model_name,
        cast({{ mb_to_gb(round_gpu_mb('gpu_memory_mb')) }} as int64)           as gpu_memory_gb,
        gpu_tdp_watts,
        machine_gpus                                                           as number_of_gpus,
        round(gpu_max_cuda_version_supported, 1)                               as gpu_max_cuda_version_supported,
        (gpu_tflops / number_of_gpus)                                          as tflops_per_gpu,
        (gpu_tflops / number_of_gpus) * machine_gpus                           as total_system_tflops,
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
