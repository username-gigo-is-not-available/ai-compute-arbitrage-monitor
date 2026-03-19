import logging
from http import HTTPStatus
from typing import Any

import certifi
import requests
from bs4 import BeautifulSoup

from config.seeds.evn import EVNConfig
from config.http import HttpConfig
from config.loader import BronzeConfigLoader
from ingestion.base import BatchIngestor
from ingestion.models.electricity_tariff_schedule import ElectricityTariffSchedule
from ingestion.models.enums import TariffType


class ElectricityTariffScheduleSeed(BatchIngestor):

    def __init__(self, config: EVNConfig, http_config: HttpConfig, name: str = None):
        super().__init__(config=config, name=name)
        self.http_config = http_config
        self.schedule: list[dict[str, Any]] = [
            *[{"day_of_week": day, "tariff_type": TariffType.LOW, "start_hour": 0, "end_hour": 7} for day in
              range(1, 6)],
            *[{"day_of_week": day, "tariff_type": TariffType.HIGH, "start_hour": 7, "end_hour": 13} for day in
              range(1, 6)],
            *[{"day_of_week": day, "tariff_type": TariffType.LOW, "start_hour": 13, "end_hour": 15} for day in
              range(1, 6)],
            *[{"day_of_week": day, "tariff_type": TariffType.HIGH, "start_hour": 15, "end_hour": 22} for day in
              range(1, 6)],
            *[{"day_of_week": day, "tariff_type": TariffType.LOW, "start_hour": 22, "end_hour": 24} for day in
              range(1, 6)],
            {"day_of_week": 6, "tariff_type": TariffType.LOW, "start_hour": 0, "end_hour": 7},
            {"day_of_week": 6, "tariff_type": TariffType.HIGH, "start_hour": 7, "end_hour": 22},
            {"day_of_week": 6, "tariff_type": TariffType.LOW, "start_hour": 22, "end_hour": 24},
            {"day_of_week": 7, "tariff_type": TariffType.LOW, "start_hour": 0, "end_hour": 24},
        ]

    def load(self) -> list[ElectricityTariffSchedule]:
        url: str = self.config.url
        timeout_seconds: int = self.http_config.timeout_seconds
        expected_text: str = self.config.expected_schedule_text
        response = requests.get(url, timeout=timeout_seconds, verify=certifi.where())
        status: int = response.status_code
        if status != HTTPStatus.OK:
            self.logger.error(f"Electricity tariffs schedule load returned HTTP status: {status}.")
            return []

        parser: BeautifulSoup = BeautifulSoup(response.text, "html.parser")
        schedule_tag = parser.select_one(self.config.schedule_text_selector)
        if not schedule_tag:
            self.logger.warning("Could not find schedule text element on page.")
            return []

        scraped_text = next(schedule_tag.strings).strip()
        if scraped_text != expected_text.strip():
            self.logger.warning(
                f"Tariff schedule text has changed — manual review required.\n"
                f"Expected: {expected_text}\n"
                f"Got:      {scraped_text}"
            )

        return [record for row in self.schedule if (record := self.parse(data=row))]

    def parse(self, **kwargs) -> ElectricityTariffSchedule | None:
        row: dict = kwargs.get("data")
        try:
            return ElectricityTariffSchedule(
                day_of_week=row["day_of_week"],
                tariff_type=row["tariff_type"],
                start_hour=row["start_hour"],
                end_hour=row["end_hour"],
            )
        except (KeyError, ValueError, TypeError) as e:
            self.logger.warning(f"Could not parse schedule row {row}: {e}")
            return None


def main():
    loader: BronzeConfigLoader = BronzeConfigLoader()
    config: EVNConfig = loader.get_evn()
    if not config.enabled:
        return
    schedule_seed = ElectricityTariffScheduleSeed(config=config, http_config=loader.get_http())
    logging.info(f"Starting seed {schedule_seed.name}...")
    schedule_seed.run()


if __name__ == "__main__":
    main()
