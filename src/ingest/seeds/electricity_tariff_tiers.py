import logging
from datetime import datetime, UTC
from typing import Any

import requests
from http import HTTPStatus

import certifi
from bs4 import BeautifulSoup, Tag

from common.enums import DataStageType
from config.seeds.erc import ERCConfig
from config.http import HttpConfig
from config.loader import ConfigLoader
from ingest.base import SyncBatchIngestor
from ingest.models.electricity_tariff_tier import ElectricityTariffTier


class ElectricityTariffTiersSeed(SyncBatchIngestor):

    def __init__(self, config: ERCConfig, http_config: HttpConfig, name: str = None):
        super().__init__(config=config, name=name)
        self.config = config
        self.http_config = http_config

    def load(self) -> list[ElectricityTariffTier]:
        url: str = self.config.url
        data: dict[str, Any] = self.config.data
        timeout_seconds: int = self.http_config.timeout_seconds
        response = requests.post(
            url,
            data=data,
            timeout=timeout_seconds,
            verify=certifi.where()
        )
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
                electricity_tariffs.append(
                    electricity_tariff
                )
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
    erc_config: ERCConfig = loader.get_erc(stage=DataStageType.BRONZE)
    if not erc_config.enabled:
        return

    electricity_tariff: ElectricityTariffTiersSeed = ElectricityTariffTiersSeed(config=erc_config, http_config=loader.get_http())
    logging.info(f"Starting seed {electricity_tariff.name}...")
    electricity_tariff.run()

def run():
    main()

if __name__ == "__main__":
    run()