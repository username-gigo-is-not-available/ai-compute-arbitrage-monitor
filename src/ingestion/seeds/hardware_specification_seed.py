import json
import logging
import os
from pathlib import Path
from typing import Any

from config.loader import BronzeConfigLoader
from ingestion.base import BatchIngestor
from ingestion.models.enums import HardwareComponentType
from ingestion.models.hardware_specification import GPUSpecification, CPUSpecification
from ingestion.models.types import OpenDBConfig, HardwareSpecification


class HardwareSpecificationSeed(BatchIngestor):

    def __init__(self, config: OpenDBConfig,
                 hardware_component_type: HardwareComponentType,
                 name: str = None):
        super().__init__(config=config, name=name)
        self.hardware_component_type = hardware_component_type
        self.model = CPUSpecification if hardware_component_type == HardwareComponentType.CPU else GPUSpecification

    @classmethod
    def _load_files(cls, folder: Path) -> list[dict[str, Any]]:
        data: list[dict[str, Any]] = []
        for file_name in os.listdir(folder):
            file_path: Path = folder / file_name
            with open(file_path) as f:
                data.append(json.load(f))
        return data

    def load(self) -> list[HardwareSpecification]:
        folder: Path = self.config.open_db_path
        hardware_specifications: list[HardwareSpecification] = []
        data: list[dict[str, Any]] = self._load_files(folder)
        for row in data:
            parsed = self.parse(data=row)
            if parsed:
                hardware_specifications.append(parsed)
        self.logger.info(f"Parsed {len(hardware_specifications)} {self.config.subdirectory_name} specs")
        return hardware_specifications

    def parse(self, **kwargs) -> HardwareSpecification | None:
        data: dict = kwargs.get("data")
        try:
            return self.model(
                open_db_id=data["opendb_id"],
                **self._extract_fields(data)
            )
        except (KeyError, ValueError, TypeError) as e:
            self.logger.warning(f"Could not parse {self.config.subdirectory_name} spec {data["opendb_id"]}: {e}")
            return None

    def _extract_fields(self, data: dict) -> dict[str, Any]:
        metadata = data.get("metadata", {})
        if self.hardware_component_type == HardwareComponentType.GPU:
            return dict(
                model_name=metadata.get("name"),
                manufacturer_name=metadata.get("manufacturer"),
                chipset_manufacturer_name=data.get("chipset_manufacturer"),
                chipset_name=data.get("chipset"),
                memory_gb=data.get("memory"),
                memory_type=data.get("memory_type"),
                core_base_clock_mhz=data.get("core_base_clock"),
                core_boost_clock_mhz=data.get("core_boost_clock"),
                memory_bus_bits=data.get("memory_bus"),
                tdp_watts=data.get("tdp"),
                interface_type=data.get("interface"),
            )
        elif self.hardware_component_type == HardwareComponentType.CPU:
            specifications = data.get("specifications", {})
            return dict(
                model_name=metadata.get("name"),
                manufacturer_name=metadata.get("manufacturer"),
                socket_type=data.get("socket"),
                number_of_cores=data.get("cores", {}).get("total"),
                number_of_threads=data.get("cores", {}).get("threads"),
                base_clock_ghz=data.get("clocks", {}).get("performance", {}).get("base"),
                boost_clock_ghz=data.get("clocks", {}).get("performance", {}).get("boost"),
                tdp_watts=specifications.get("tdp"),
                microarchitecture_type=data.get("microarchitecture"),
            )
        else:
            raise ValueError(f"Invalid component type: {self.hardware_component_type}")


def main():
    loader: BronzeConfigLoader = BronzeConfigLoader()
    open_db_cpu_config: OpenDBConfig = loader.get_open_db_cpu()
    open_db_gpu_config: OpenDBConfig = loader.get_open_db_gpu()
    seeds: list[HardwareSpecificationSeed] = []

    if open_db_cpu_config.enabled:
        seeds.append(
            HardwareSpecificationSeed(config=open_db_cpu_config, hardware_component_type=HardwareComponentType.CPU)
        )
    if open_db_gpu_config.enabled:
        seeds.append(
            HardwareSpecificationSeed(config=open_db_gpu_config, hardware_component_type=HardwareComponentType.GPU)
        )

    for seed in seeds:
        logging.info(f"Starting seed {seed.name}...")
        seed.run()


if __name__ == '__main__':
    main()
