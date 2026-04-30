from pydantic import field_validator

from ingest.models.base import BaseRecord
from common.enums import VerificationFlag, OfferType


class VastAIOffer(BaseRecord):
    # IDs
    offer_id: int
    machine_id: int
    host_id: int
    # TYPES
    offer_type: OfferType
    # PRICES
    total_price_usd_per_hr: float
    gpu_price_usd_per_hr: float
    minimum_bid_price_usd: float | None
    storage_cost_usd_per_hr: float | None = None
    network_upload_cost_usd_per_gbit: float | None = None
    network_download_cost_usd_per_gbit: float | None = None
    deep_learning_score_per_usd: float
    # GPU
    gpu_architecture: str
    gpu_model_name: str
    gpu_memory_mb: float
    gpu_tdp_watts: float | None = None
    number_of_gpus: int = 1
    gpu_max_cuda_version_supported: float | None = None
    gpu_tflops: float | None = None
    gpu_bandwidth_gbytes_per_sec: float | None = None
    # CPU
    cpu_architecture: str | None = None
    cpu_model_name: str | None = None
    number_of_cpu_cores: float | None = None
    cpu_clock_speed_ghz: float | None = None
    # RAM
    ram_mb: float
    # DISK
    disk_model_name: str | None = None
    disk_space_gb: float
    disk_bandwidth_mbytes_per_sec: float | None = None
    # PCIe
    pcie_generation: float | None
    pcie_bandwidth_gbytes_per_sec: float | None
    # INTERNET
    network_download_mbits_per_sec: float
    network_upload_mbits_per_sec: float
    # OTHER METRICS
    reliability_score: float | None = None
    deep_learning_score: float | None = None
    # LOCATION
    geolocation: str | None = None
    # FLAGS
    verification_flag: VerificationFlag
    rentable_flag: bool
    rented_flag: bool

    @field_validator("*", mode="before")
    @classmethod
    def empty_string_to_none(cls, v):
        if isinstance(v, str) and v.strip() == "":
            return None
        return v