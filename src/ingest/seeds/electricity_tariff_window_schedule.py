import logging
from dataclasses import dataclass
from datetime import datetime, UTC

from common.classes import Dataset
from common.enums import DatasetType, DatasetName
from config.apis.evn import EVNConfig
from config.http import HttpConfig
from config.loader import ConfigLoader
from config.storage import GCPStorageConfig
from ingest.evn_base import EVNBaseIngestor
from ingest.models.electricity_tariff_window_schedule import ElectricityTariffWindowSchedule


@dataclass
class ElectricityTariffWindowScheduleIngestor(EVNBaseIngestor):
    http_config: HttpConfig

    def load(self) -> list[ElectricityTariffWindowSchedule]:
        parser = self.fetch_soup(self.config.tariff_tiers_url)
        if parser is None:
            return []
        ingested_at: datetime = datetime.now(UTC)
        valid_from_text: str = parser.select_one(self.config.tariff_tiers_valid_from_text_selector).text.strip()
        schedule_text: str = parser.select_one(self.config.schedule_text_selector).text.strip()

        electricity_tariff_schedule: ElectricityTariffWindowSchedule = self.parse(
            timestamp=ingested_at,
            schedule_text=schedule_text,
            valid_from_text=valid_from_text,
        )
        if electricity_tariff_schedule is None:
            return []
        return [electricity_tariff_schedule]

    def parse(self, **kwargs) -> ElectricityTariffWindowSchedule | None:
        row: dict = kwargs.get("data")
        ingested_at: datetime = kwargs.get("timestamp")
        schedule_text: str = kwargs.get("schedule_text")
        valid_from_text: str = kwargs.get("valid_from_text")
        try:
            return ElectricityTariffWindowSchedule(
                ingested_at=ingested_at,
                schedule_text=schedule_text,
                valid_from_text=valid_from_text
            )
        except (KeyError, ValueError, TypeError) as e:
            self.logger.warning(f"Could not parse schedule row {row}: {e}")
            return None


def main():
    loader: ConfigLoader = ConfigLoader()
    evn_config: EVNConfig = loader.get_evn()
    storage_config: GCPStorageConfig = loader.get_storage()
    electricity_tariff_window_schedule_dataset: Dataset = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE, dataset_type=DatasetType.SEEDS)
    if not evn_config.enabled:
        return

    electricity_tariff_window_schedule_ingestor = ElectricityTariffWindowScheduleIngestor(
        dataset=electricity_tariff_window_schedule_dataset,
        config=evn_config,
        storage_config=storage_config,
        http_config=loader.get_http(),
    )
    logging.info(f"Starting seed {electricity_tariff_window_schedule_ingestor.name}...")
    electricity_tariff_window_schedule_ingestor.run()


def run():
    main()


if __name__ == "__main__":
    run()