from dataclasses import dataclass
from datetime import UTC, datetime

from bs4 import Tag

from common.enums import ConsumerCategoryType
from ingest.evn_base import EVNBaseIngestor
from ingest.models.electricity_tariff_fee import ElectricityTariffFee


@dataclass(frozen=True)
class CellSpecification:
    consumer_category: ConsumerCategoryType
    label_idx: int
    metric_idx: int
    value_idx: int


@dataclass
class ElectricityTariffFeesIngestor(EVNBaseIngestor):

    def load(self) -> list[ElectricityTariffFee]:
        parser = self.fetch_soup(self.config.tariff_tiers_url)
        ingested_at: datetime = datetime.now(UTC)
        valid_from_text: str = parser.select_one(self.config.tariff_tiers_valid_from_text_selector).text.strip()

        cell_specifications: list[CellSpecification] = [
            CellSpecification(
                consumer_category=ConsumerCategoryType.HOUSEHOLD,
                label_idx=13,
                metric_idx=14,
                value_idx=19,
            ),
            CellSpecification(
                consumer_category=ConsumerCategoryType.BUSINESS,
                label_idx=13,
                metric_idx=14,
                value_idx=18,
            ),
            CellSpecification(
                consumer_category=ConsumerCategoryType.HOUSEHOLD,
                label_idx=28,
                metric_idx=29,
                value_idx=34,
            ),
            CellSpecification(
                consumer_category=ConsumerCategoryType.BUSINESS,
                label_idx=28,
                metric_idx=29,
                value_idx=33,
            ),
        ]

        electricity_tariff_fees: list[ElectricityTariffFee] = []

        table_element: Tag = parser.select_one(self.config.tariff_fees_table_selector)
        cells: list[Tag] = table_element.select(self.config.table_cells_selector)
        for cell_specification in cell_specifications:
            electricity_tariff_fee: ElectricityTariffFee = self.parse(cells=cells,
                                                                      specification=cell_specification,
                                                                      valid_from_text=valid_from_text,
                                                                      ingested_at=ingested_at)
            electricity_tariff_fees.append(electricity_tariff_fee)

        return electricity_tariff_fees

    def parse(self, cells: list[Tag], specification: CellSpecification, valid_from_text: str,
              ingested_at: datetime) -> ElectricityTariffFee:
        return ElectricityTariffFee(
            ingested_at=ingested_at,
            consumer_category=specification.consumer_category,
            label=cells[specification.label_idx].text.strip(),
            metric=cells[specification.metric_idx].text.strip(),
            value=cells[specification.value_idx].text.strip(),
            valid_from_text=valid_from_text
        )
