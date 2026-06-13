import logging
from dataclasses import dataclass, field
from datetime import datetime, UTC
from http import HTTPStatus
from typing import Any

import certifi
import requests
from bs4 import BeautifulSoup

from common.classes import Dataset
from common.enums import TariffType, DatasetType, DatasetName
from config.apis.evn import EVNConfig
from config.http import HttpConfig
from config.loader import ConfigLoader
from config.storage import GCPStorageConfig
from ingest.base import SyncIngestor
from ingest.models.electricity_tariff_schedule import ElectricityTariffSchedule


@dataclass
class ElectricityTariffScheduleSeed(SyncIngestor):
    http_config: HttpConfig
    schedule: list[dict[str, Any]] = field(init=False)

    def __post_init__(self):
        super().__post_init__()
        self.schedule = [
            *[{"day_of_week": day, "tariff_type": TariffType.LOW,  "start_hour": 0,  "end_hour": 7,  "valid_from": "01.01.2026"} for day in range(1, 7)],
            *[{"day_of_week": day, "tariff_type": TariffType.HIGH, "start_hour": 7,  "end_hour": 13, "valid_from": "01.01.2026"} for day in range(1, 7)],
            *[{"day_of_week": day, "tariff_type": TariffType.LOW,  "start_hour": 13, "end_hour": 15, "valid_from": "01.01.2026"} for day in range(1, 7)],
            *[{"day_of_week": day, "tariff_type": TariffType.HIGH, "start_hour": 15, "end_hour": 22, "valid_from": "01.01.2026"} for day in range(1, 7)],
            *[{"day_of_week": day, "tariff_type": TariffType.LOW,  "start_hour": 22, "end_hour": 24, "valid_from": "01.01.2026"} for day in range(1, 7)],
            {"day_of_week": 7, "tariff_type": TariffType.LOW, "start_hour": 0, "end_hour": 24, "valid_from": "01.01.2026"},
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

        scraped_text: str = ' '.join(schedule_tag.text.split())
        if scraped_text != expected_text.strip():
            self.logger.warning(
                f"Tariff schedule text has changed — manual review required.\n"
                f"Expected: {expected_text}\n"
                f"Got:      {scraped_text}"
            )
            return []

        ingested_at: datetime = datetime.now(UTC)
        return [record for row in self.schedule if (record := self.parse(data=row, timestamp=ingested_at))]

    def parse(self, **kwargs) -> ElectricityTariffSchedule | None:
        row: dict = kwargs.get("data")
        ingested_at: datetime = kwargs.get("timestamp")
        try:
            return ElectricityTariffSchedule(
                ingested_at=ingested_at,
                day_of_week=row["day_of_week"],
                tariff_type=row["tariff_type"],
                start_hour=row["start_hour"],
                end_hour=row["end_hour"],
                valid_from=row["valid_from"],
            )
        except (KeyError, ValueError, TypeError) as e:
            self.logger.warning(f"Could not parse schedule row {row}: {e}")
            return None


def main():
    loader: ConfigLoader = ConfigLoader()
    evn_config: EVNConfig = loader.get_evn()
    storage_config: GCPStorageConfig = loader.get_storage()
    electricity_tariff_schedule: Dataset = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_SCHEDULE, dataset_type=DatasetType.SEEDS)
    if not evn_config.enabled:
        return

    schedule_seed = ElectricityTariffScheduleSeed(
        dataset=electricity_tariff_schedule,
        config=evn_config,
        storage_config=storage_config,
        http_config=loader.get_http(),
    )
    logging.info(f"Starting seed {schedule_seed.name}...")
    schedule_seed.run()


def run():
    main()


if __name__ == "__main__":
    run()