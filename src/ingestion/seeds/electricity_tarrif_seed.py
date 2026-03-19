import logging
from typing import Any

import requests
from http import HTTPStatus

import certifi
from bs4 import BeautifulSoup, Tag

from config.seeds.erc import ERCConfig
from config.http import HttpConfig
from config.loader import BronzeConfigLoader
from ingestion.base import BatchIngestor
from ingestion.models.electricity_tariff import ElectricityTariff


class ElectricityTariffSeed(BatchIngestor):

    def __init__(self, config: ERCConfig, http_config: HttpConfig, name: str = None):
        super().__init__(config=config, name=name)
        self.config = config
        self.http_config = http_config

    def load(self) -> list[ElectricityTariff]:
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
            self.logger.error(f"Electricity tariffs load returned HTTP status: {status}.")
            return []
        parser: BeautifulSoup = BeautifulSoup(response.text, "html.parser")
        electricity_tariffs: list[ElectricityTariff] = []

        for row in parser.select(self.config.table_rows_selector):
            electricity_tariff: ElectricityTariff = self.parse(data=row)
            if electricity_tariff:
                electricity_tariffs.append(
                    electricity_tariff
                )
        return electricity_tariffs

    def parse(self, **kwargs) -> ElectricityTariff | None:
        tag: Tag = kwargs.get('data')
        try:
            return ElectricityTariff(
                tariff_type=tag.select_one(self.config.type_selector).text,
                price_per_kwh_mkd=tag.select_one(self.config.price_selector).text,
                valid_from=tag.select_one(self.config.valid_from_selector).text,
            )
        except (KeyError, ValueError, TypeError) as e:
            self.logger.warning(f"Could not parse electricity tariff: {tag.text} {e}")
            return None


def main():
    loader: BronzeConfigLoader = BronzeConfigLoader()
    config: ERCConfig = loader.get_erc()
    if not config.enabled:
        return
    electricity_tariff: ElectricityTariffSeed = ElectricityTariffSeed(config=config, http_config=loader.get_http())
    logging.info(f"Starting seed {electricity_tariff.name}...")
    electricity_tariff.run()


if __name__ == '__main__':
    main()