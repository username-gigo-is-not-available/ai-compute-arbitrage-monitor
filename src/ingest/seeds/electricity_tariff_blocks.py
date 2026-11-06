from dataclasses import dataclass
from datetime import UTC, datetime

from bs4 import Tag

from common.enums import ConsumerCategoryType, TariffWindowType
from ingest.evn_base import EVNBaseIngestor
from ingest.models.electricity_tariff_block import ElectricityTariffBlock


@dataclass
class ElectricityTariffBlocksIngestor(EVNBaseIngestor[ElectricityTariffBlock]):
    def load(self) -> list[ElectricityTariffBlock]:
        parser = self.fetch_soup(self.config.tariff_system_url)
        ingested_at: datetime = datetime.now(UTC)
        valid_from_text: str = self.select_required(parser, self.config.tariff_blocks_valid_from_selector).text.strip()

        electricity_tariff_blocks: list[ElectricityTariffBlock] = []

        paragraph_element: Tag = self.select_required(parser, self.config.tariff_blocks_paragraph_selector)
        rows: list[Tag] = paragraph_element.find_all(self.config.tariff_blocks_strong_selector)[
            : self.config.tariff_blocks_limit
        ]
        for row in rows:
            electricity_tariff_block: ElectricityTariffBlock = self.parse(
                row=row,
                valid_from_text=valid_from_text,
                ingested_at=ingested_at,
            )
            electricity_tariff_blocks.append(electricity_tariff_block)

        return electricity_tariff_blocks

    def parse(self, row: Tag, valid_from_text: str, ingested_at: datetime) -> ElectricityTariffBlock:
        return ElectricityTariffBlock(
            ingested_at=ingested_at,
            consumer_category=ConsumerCategoryType.HOUSEHOLD,
            tariff_window_type=TariffWindowType.HIGH,
            tariff_block_number_text=row.text,
            kwh_boundaries_text=self.next_sibling_required(row).text,
            valid_from_text=valid_from_text,
        )
