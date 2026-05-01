import logging
import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv

from config.cluster import GCPClusterConfig
from config.http import HttpConfig
from config.storage import GCPStorageConfig
from config.seeds.erc import ERCConfig
from config.seeds.evn import EVNConfig
from config.sources.exchange_rate import ExchangeRateConfig
from config.sources.vast_ai import VastAIConfig
from common.enums import DataStageType

load_dotenv()


class ConfigLoader:
    def __init__(self, config_path: Path = Path(os.getenv("SETTINGS_PATH", "settings.yaml"))):
        self._raw = self._load_yaml(config_path)
        self._setup_logging()
        self._paths = self.get_storage()

    def get_cluster(self) -> GCPClusterConfig:
        gcp_config: dict[str, Any] = self._raw["gcp"]
        return GCPClusterConfig(
            project_id=gcp_config["project_id"],
            region_name=gcp_config["region_name"],
            image_tag=gcp_config["dataproc"]["image_tag"],
            runtime_packages=gcp_config["dataproc"]["runtime_packages"],
            entrypoints_path=gcp_config["dataproc"]["entrypoints_path"],
            subnetwork_name=gcp_config["dataproc"]["subnetwork_name"],
            service_account_email=gcp_config["dataproc"]["service_account_email"],
        )


    def get_http(self) -> HttpConfig:
        return HttpConfig(**self._raw["http"])


    def get_storage(self) -> GCPStorageConfig :
        path_data: dict[str, Any] = self._raw["paths"]
        return GCPStorageConfig(
            seeds_directory_name=path_data["seeds_directory_name"],
            sources_directory_name=path_data["sources_directory_name"],
            bucket_name=self._raw["gcp"]["gcs"]["bucket_name"]
        )


    def _setup_logging(self) -> None:
        log_config = self._raw.get("logging", {})
        logging.basicConfig(
            level=log_config.get("level", "INFO"),
            format=log_config.get("format", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"),
            force=True
        )


    @staticmethod
    def _load_yaml(path: Path) -> dict:
        try:
            with open(path, "r", encoding="utf-8") as f:
                return yaml.safe_load(f) or {}
        except FileNotFoundError:
            logging.warning(f"Config file not found at {path}, using defaults.")
            return {}
        except Exception as e:
            logging.error(f"Could not load config file {path}: {e}")
            return {}


class BronzeConfigLoader(ConfigLoader):
    def __init__(self, config_path: Path = Path(os.getenv("SETTINGS_PATH", "settings.yaml"))):
        super().__init__(config_path)

    def get_vast_ai(self) -> VastAIConfig:
        return VastAIConfig(
            **self._raw["sources"]["vast_ai"],
            output_directory_path=self._paths.sources_directory_path_for_stage(DataStageType.BRONZE)
        )

    def get_exchange_rate(self) -> ExchangeRateConfig:
        return ExchangeRateConfig(
            **self._raw["sources"]["exchange_rate"],
            output_directory_path=self._paths.sources_directory_path_for_stage(DataStageType.BRONZE)
        )

    def get_erc(self) -> ERCConfig:
        return ERCConfig(
            **self._raw["seeds"]["erc"],
            output_directory_path=self._paths.seeds_directory_path_for_stage(DataStageType.BRONZE)
        )

    def get_evn(self) -> EVNConfig:
        return EVNConfig(
            **self._raw["seeds"]["evn"],
            output_directory_path=self._paths.seeds_directory_path_for_stage(DataStageType.BRONZE)
        )


class SilverConfigLoader(ConfigLoader):
    def __init__(self, config_path: Path = Path(os.getenv("SETTINGS_PATH", "settings.yaml"))):
        super().__init__(config_path)

    def get_vast_ai(self) -> VastAIConfig:
        return VastAIConfig(**self._raw["sources"]["vast_ai"],
                            input_directory_path=self._paths.sources_directory_path_for_stage(DataStageType.BRONZE),
                            output_directory_path=self._paths.sources_directory_path_for_stage(DataStageType.SILVER),
                            )

    def get_exchange_rate(self) -> ExchangeRateConfig:
        return ExchangeRateConfig(**self._raw["sources"]["exchange_rate"],
                                  input_directory_path=self._paths.sources_directory_path_for_stage(
                                      DataStageType.BRONZE),
                                  output_directory_path=self._paths.sources_directory_path_for_stage(
                                      DataStageType.SILVER),
                                  )

    def get_erc(self) -> ERCConfig:
        return ERCConfig(
            **self._raw["seeds"]["erc"],
            input_directory_path=self._paths.seeds_directory_path_for_stage(DataStageType.BRONZE),
            output_directory_path=self._paths.seeds_directory_path_for_stage(DataStageType.SILVER)
        )

    def get_evn(self) -> EVNConfig:
        return EVNConfig(
            **self._raw["seeds"]["evn"],
            input_directory_path=self._paths.seeds_directory_path_for_stage(DataStageType.BRONZE),
            output_directory_path=self._paths.seeds_directory_path_for_stage(DataStageType.SILVER)
        )
