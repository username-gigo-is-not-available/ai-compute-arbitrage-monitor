# models/vast_ai_offer.py
from datetime import datetime, UTC
from pydantic import Field, field_validator

from ingestion.models.base import BaseRecord
from ingestion.models.enums import VerificationFlag


class VastAIOffer(BaseRecord):
    # IDs
    instance_id: int
    # PRICES
    total_price_usd_per_hour: float
    gpu_price_usd_per_hour: float
    deep_learning_score_per_dollar: float
    # GPU
    gpu_architecture: str
    gpu_model_name: str
    gpu_memory_mb: float
    gpu_max_power_watts: float | None = None
    number_of_gpus: int = 1
    gpu_max_cuda_version_supported: float | None = None
    gpu_tflops: float | None = None
    # CPU
    cpu_architecture: str | None = None
    cpu_model_name: str | None = None
    number_of_cpu_cores: float | None = None
    # RAM
    ram_mb: float
    # DISK
    disk_model_name: str | None = None
    disk_space_gb: float
    # INTERNET
    network_download_mbps: float
    network_upload_mbps: float
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