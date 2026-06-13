import logging
from dataclasses import dataclass
from datetime import datetime, UTC
from http import HTTPStatus
from typing import Any

import certifi
import requests
from bs4 import BeautifulSoup, Tag

from common.classes import Dataset
from common.enums import DatasetName, DatasetType
from config.apis.erc import ERCConfig
from config.http import HttpConfig
from config.loader import ConfigLoader
from config.storage import GCPStorageConfig
from ingest.base import SyncIngestor
from ingest.models.electricity_tariff_tier import ElectricityTariffTier


@dataclass
class ElectricityTariffTiersIngestor(SyncIngestor):
    http_config: HttpConfig

    def load(self) -> list[ElectricityTariffTier]:
        url: str = self.config.url
        data: dict[str, Any] = self.config.data
        timeout_seconds: int = self.http_config.timeout_seconds
        response = requests.post(url, data=data, timeout=timeout_seconds, verify=certifi.where())
        status: int = response.status_code
        if status != HTTPStatus.OK:
            self.logger.error(f"Electricity tariff prices load returned HTTP status: {status}.")
            return []
        parser: BeautifulSoup = BeautifulSoup(response.text, "html.parser")
        electricity_tariffs: list[ElectricityTariffTier] = []
        ingested_at: datetime = datetime.now(UTC)
        for row in parser.select(self.config.table_rows_selector):
            electricity_tariff: ElectricityTariffTier = self.parse(data=row, timestamp=ingested_at)
            if electricity_tariff:
                electricity_tariffs.append(electricity_tariff)
        return electricity_tariffs

    def parse(self, **kwargs) -> ElectricityTariffTier | None:
        tag: Tag = kwargs.get('data')
        ingested_at: datetime = kwargs.get('timestamp')
        try:
            return ElectricityTariffTier(
                ingested_at=ingested_at,
                tariff_description=tag.select_one(self.config.description_selector).text,
                price_mkd_per_kwh=tag.select_one(self.config.price_selector).text,
                valid_from=tag.select_one(self.config.valid_from_selector).text,
            )
        except (KeyError, ValueError, TypeError) as e:
            self.logger.warning(f"Could not parse electricity tariff price: {tag.text} {e}")
            return None


def main():
    loader: ConfigLoader = ConfigLoader()
    erc_config: ERCConfig = loader.get_erc()
    storage_config: GCPStorageConfig = loader.get_storage()
    electricity_tariff_tiers: Dataset = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_TIERS, dataset_type=DatasetType.SEEDS)
    if not erc_config.enabled:
        return

    electricity_tariff_tiers_ingestor = ElectricityTariffTiersIngestor(
        dataset=electricity_tariff_tiers,
        config=erc_config,
        storage_config=storage_config,
        http_config=loader.get_http(),
    )
    logging.info(f"Starting seed {electricity_tariff_tiers_ingestor.name}...")
    electricity_tariff_tiers_ingestor.run()


def run():
    main()


if __name__ == "__main__":
    run()