from datetime import datetime
from typing import Any

from pydantic import field_validator

from common.enums import OfferType, VerificationStatusType
from ingest.models.base import BaseRecord


class VastAIOffer(BaseRecord):
    # The scheduled day this snapshot is for; ingested_at is the fetch moment (ADR-019)
    snapshot_at: datetime

    offer_id: int
    machine_id: int
    host_id: int

    offer_type: OfferType

    total_price_usd_per_hr: float
    gpu_price_usd_per_hr: float
    minimum_bid_price_usd: float | None
    storage_cost_usd_per_hr: float | None = None
    network_upload_cost_usd_per_gbit: float | None = None
    network_download_cost_usd_per_gbit: float | None = None
    deep_learning_score_per_usd: float

    gpu_architecture: str
    gpu_model_name: str
    gpu_memory_mb: float
    gpu_tdp_watts: float | None = None
    number_of_offer_gpus: int = 1
    gpu_fraction_of_machine: float | None = None
    gpu_ids: list[int] | None = None
    gpu_max_cuda_version_supported: float | None = None
    gpu_tflops: float | None = None
    gpu_bandwidth_gbytes_per_sec: float | None = None

    cpu_architecture: str | None = None
    cpu_model_name: str | None = None
    number_of_cpu_cores: float | None = None
    cpu_clock_speed_ghz: float | None = None

    ram_mb: float

    disk_model_name: str | None = None
    disk_space_gb: float
    disk_bandwidth_mbytes_per_sec: float | None = None

    pcie_generation: float | None
    pcie_bandwidth_gbytes_per_sec: float | None

    network_download_mbits_per_sec: float
    network_upload_mbits_per_sec: float

    reliability_score: float | None = None
    deep_learning_score: float | None = None

    geolocation: str | None = None

    verification_flag: VerificationStatusType
    rentable_flag: bool
    rented_flag: bool

    @field_validator("*", mode="before")
    @classmethod
    def empty_string_to_none(cls, v: Any) -> Any:
        if isinstance(v, str) and v.strip() == "":
            return None
        return v
