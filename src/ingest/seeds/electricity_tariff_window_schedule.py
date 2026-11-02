from dataclasses import dataclass
from datetime import datetime, UTC

from ingest.evn_base import EVNBaseIngestor
from ingest.models.electricity_tariff_window_schedule import ElectricityTariffWindowSchedule


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
