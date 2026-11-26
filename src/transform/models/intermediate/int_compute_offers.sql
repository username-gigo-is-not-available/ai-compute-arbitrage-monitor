{{
    config(
        tags = ['compute_offers']
    )
}}

-- A machine collapses into one row only when all of its offers agree on GPU model, machine size and per-GPU price
-- (ADR-020). The few that don't (13-20 per census, e.g. a 5090 next to four 4090s) are dropped rather than
-- given one model or price that is wrong for part of the machine; assert_stg_compute_offers_machine_slices_agree
-- reports them. A machine without a reported power draw (0 W, one per census) can't be costed and is dropped too;
-- assert_stg_compute_offers_gpu_tdp_reported reports it. A $0 ask is no price, so that machine is dropped as well;
-- assert_stg_compute_offers_ask_reported reports it.
with single_type_machines as (
    select
        machine_id,
        snapshot_at
    from {{ ref('stg_compute_offers') }}
    group by machine_id, snapshot_at
    having
        count(distinct gpu_model_name) = 1
        and count(distinct {{ number_of_machine_gpus() }}) = 1
        and count(distinct round(gpu_price_usd_per_hr / number_of_offer_gpus, 4)) = 1
),

offers as (
    -- An offer rents number_of_offer_gpus of its machine's number_of_machine_gpus (rented or not, ADR-020).
    select
        o.*,
        {{ number_of_machine_gpus('o.number_of_offer_gpus', 'o.gpu_fraction_of_machine') }} as number_of_machine_gpus,
        o.gpu_price_usd_per_hr / o.number_of_offer_gpus as base_price_per_gpu_usd_per_hr,
        o.total_price_usd_per_hr / o.number_of_offer_gpus as total_price_per_gpu_usd_per_hr,
        o.minimum_bid_price_usd / o.number_of_offer_gpus as minimum_bid_price_per_gpu_usd,
        o.gpu_tflops / o.number_of_offer_gpus as tflops_per_gpu
    from {{ ref('stg_compute_offers') }} as o
    inner join single_type_machines using (machine_id, snapshot_at)
),

free_gpus as (
    -- A GPU is free when an offer that can be rented now lists it; GPUs no offer lists count as taken (ADR-030).
    select
        machine_id,
        snapshot_at,
        count(distinct gpu_id) as number_of_free_gpus
    from offers, unnest(gpu_ids) as gpu_id
    where rentable_flag
    group by machine_id, snapshot_at
),

machines as (
    -- One row per machine: its largest offer carries the specs. The window columns see all of its offers,
    -- because qualify filters after they are computed.
    select
        *,
        min(minimum_bid_price_per_gpu_usd) over machine_census as lowest_minimum_bid_price_per_gpu_usd
    from offers
    qualify
        row_number() over (
            partition by machine_id, snapshot_at order by number_of_offer_gpus desc, offer_id asc
        ) = 1
    window machine_census as (partition by machine_id, snapshot_at)
),

machines_with_free_gpus as (
    select
        m.*,
        coalesce(f.number_of_free_gpus, 0) as number_of_free_gpus
    from machines as m
    left join free_gpus as f using (machine_id, snapshot_at)
),

transformed as (
    select
        machine_id,
        host_id,

        total_price_per_gpu_usd_per_hr * number_of_machine_gpus as total_price_usd_per_hr,
        base_price_per_gpu_usd_per_hr * number_of_machine_gpus as gpu_price_usd_per_hr,
        -- The largest offer may be a slice of the machine (#134). CPU, RAM and disk grow with the slice, so they
        -- scale to the machine; the DL score does not grow linearly, so it is unknown for a slice and left null.
        if(number_of_offer_gpus = number_of_machine_gpus, deep_learning_score_per_usd, null)
            as deep_learning_score_per_usd,
        lowest_minimum_bid_price_per_gpu_usd * number_of_machine_gpus as minimum_bid_price_usd,
        storage_cost_usd_per_hr,
        network_upload_cost_usd_per_gbit,
        network_download_cost_usd_per_gbit,

        gpu_architecture,
        gpu_model_name,
        cast({{ mb_to_gb(round_gpu_mb('gpu_memory_mb')) }} as int64) as gpu_memory_gb,
        gpu_tdp_watts,
        number_of_machine_gpus,
        round(gpu_max_cuda_version_supported, 1) as gpu_max_cuda_version_supported,
        tflops_per_gpu,
        tflops_per_gpu * number_of_machine_gpus as total_system_tflops,
        gpu_bandwidth_gbytes_per_sec,

        cpu_architecture,
        cpu_model_name,
        cast(round(number_of_cpu_cores * number_of_machine_gpus / number_of_offer_gpus) as int64)
            as number_of_cpu_cores,
        cpu_clock_speed_ghz,

        {{ mb_to_gb('ram_mb') }} * number_of_machine_gpus / number_of_offer_gpus as ram_gb,
        disk_model_name,
        disk_space_gb * number_of_machine_gpus / number_of_offer_gpus as disk_space_gb,
        {{ mb_to_gb('disk_bandwidth_mbytes_per_sec') }} as disk_bandwidth_gbytes_per_sec,

        pcie_generation,
        pcie_bandwidth_gbytes_per_sec,

        network_download_mbits_per_sec,
        network_upload_mbits_per_sec,

        reliability_score,
        if(number_of_offer_gpus = number_of_machine_gpus, deep_learning_score, null) as deep_learning_score,

        {{ extract_country_code('geolocation') }} as country_code,

        verification_flag,
        number_of_free_gpus,

        snapshot_at as valid_from,
        cast('9999-12-31' as timestamp) as valid_to,
        processed_at

    from machines_with_free_gpus
)

select * from transformed
where
    gpu_tdp_watts > 0
    and total_price_usd_per_hr > 0
