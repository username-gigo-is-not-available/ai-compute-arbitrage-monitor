import logging
from dataclasses import dataclass
from datetime import datetime, UTC

from common.classes import Dataset
from common.enums import DatasetType, DatasetName
from config.apis.evn import EVNConfig
from config.loader import ConfigLoader
from ingest.evn_base import EVNBaseIngestor
from ingest.models.electricity_tariff_window_schedule import ElectricityTariffWindowSchedule
from ingest.schemas.electricity_tariff_window_schedule import ELECTRICITY_TARIFF_WINDOW_SCHEDULE_BRONZE_SCHEMA
from ingest.write_strategy import AppendByDay, BronzeTable


@dataclass
class ElectricityTariffWindowScheduleIngestor(EVNBaseIngestor):

    def load(self) -> list[ElectricityTariffWindowSchedule]:
        parser = self.fetch_soup(self.config.tariff_tiers_url)
        ingested_at: datetime = datetime.now(UTC)
        valid_from_text: str = parser.select_one(self.config.tariff_tiers_valid_from_text_selector).text.strip()
        schedule_text: str = parser.select_one(self.config.schedule_text_selector).text.strip()

        electricity_tariff_window_schedule: ElectricityTariffWindowSchedule = self.parse(
            schedule_text=schedule_text,
            valid_from_text=valid_from_text,
            ingested_at=ingested_at,
        )
        return [electricity_tariff_window_schedule]

    def parse(self, schedule_text: str, valid_from_text: str, ingested_at: datetime) -> ElectricityTariffWindowSchedule:
        return ElectricityTariffWindowSchedule(
            ingested_at=ingested_at,
            schedule_text=schedule_text,
            valid_from_text=valid_from_text
        )


def main() -> None:
    loader: ConfigLoader = ConfigLoader()
    evn_config: EVNConfig = loader.get_evn()
    electricity_tariff_window_schedule_dataset: Dataset = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE, dataset_type=DatasetType.SEEDS)
    if not evn_config.enabled:
        return

    electricity_tariff_window_schedule_ingestor = ElectricityTariffWindowScheduleIngestor(
        dataset=electricity_tariff_window_schedule_dataset,
        config=evn_config,
        lakehouse_config=loader.get_lakehouse(),
        bronze_table=BronzeTable(ELECTRICITY_TARIFF_WINDOW_SCHEDULE_BRONZE_SCHEMA, AppendByDay(column="ingested_at")),
        http_config=loader.get_http(),
    )
    logging.info(f"Starting seed {electricity_tariff_window_schedule_ingestor.name}...")
    electricity_tariff_window_schedule_ingestor.run()


def run(scheduled_at: str | None = None) -> None:
    main()


if __name__ == "__main__":
    run()