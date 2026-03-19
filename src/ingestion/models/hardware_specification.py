
from ingestion.models.base import BaseRecord


class HardwareSpecification(BaseRecord):
    open_db_id: str
    model_name: str
    manufacturer_name: str | None
    tdp_watts: int | None


class GPUSpecification(HardwareSpecification):
    chipset_manufacturer_name: str | None
    chipset_name: str | None
    memory_gb: int | None
    memory_type: str | None
    memory_bus_bits: int | None
    core_base_clock_mhz: float | None
    core_boost_clock_mhz: float | None
    interface_type: str | None


class CPUSpecification(HardwareSpecification):
    socket_type: str | None
    number_of_cores: int | None
    number_of_threads: int | None
    base_clock_ghz: float | None
    boost_clock_ghz: float | None
    microarchitecture_type: str | None