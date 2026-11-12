{{ config(
    materialized = 'table',
    tags         = ['marts'],
    cluster_by   = ['gpu_model_name', 'gpu_memory_gb']
) }}
-- The machines behind each mart_gpu_profitability row, so a host can see what machines like theirs ask. Nothing here
-- depends on the tariff, so there is no tier and no cost; money per tier is in mart_gpu_profitability (ADR-027).
-- Unranked: ranking single listings floats unsold overpriced asks to the top (a 4090 at $40/hr).
select distinct
    machine_id,
    host_id,

    country_code,
    reliability_score,
    verification_flag,
    rentable_flag,

    gpu_architecture,
    gpu_model_name,
    gpu_memory_gb,
    number_of_machine_gpus,
    tflops_per_gpu,
    total_system_tflops,
    gpu_tdp_watts,
    gpu_bandwidth_gbytes_per_sec,
    gpu_max_cuda_version_supported,

    cpu_architecture,
    cpu_model_name,
    number_of_cpu_cores,
    cpu_clock_speed_ghz,

    ram_gb,
    disk_space_gb,
    disk_bandwidth_gbytes_per_sec,

    pcie_generation,
    pcie_bandwidth_gbytes_per_sec,

    network_download_mbits_per_sec,
    network_upload_mbits_per_sec,
    network_download_cost_usd_per_gbit,
    network_upload_cost_usd_per_gbit,

    kwh_per_tflop,
    deep_learning_score,
    deep_learning_score_per_usd,

    revenue_usd_per_hr as ask_usd_per_hr,
    revenue_per_gpu_usd_per_hr as ask_per_gpu_usd_per_hr,
    gpu_price_usd_per_hr,
    minimum_bid_price_usd,
    storage_cost_usd_per_hr,

    valid_from

from {{ ref('fct_compute_offers') }}
where cast(valid_to as date) = date '9999-12-31'
