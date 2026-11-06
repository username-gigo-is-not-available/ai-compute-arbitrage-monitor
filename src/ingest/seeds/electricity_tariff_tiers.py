from dataclasses import dataclass
from datetime import UTC, datetime

from bs4 import Tag

from common.enums import ConsumerCategoryType
from ingest.evn_base import EVNBaseIngestor
from ingest.models.electricity_tariff_tier import ElectricityTariffTier


@dataclass(frozen=True)
class TableSpecification:
    selector: str
    consumer_category: ConsumerCategoryType
    row_width: int
    label_idx: int
    metric_idx: int
    value_idx: int
    tariff_tier_idx: int


@dataclass
class ElectricityTariffTiersIngestor(EVNBaseIngestor[ElectricityTariffTier]):
    def load(self) -> list[ElectricityTariffTier]:
        parser = self.fetch_soup(self.config.tariff_tiers_url)
        ingested_at: datetime = datetime.now(UTC)
        valid_from_text: str = self.select_required(
            parser, self.config.tariff_tiers_valid_from_text_selector
        ).text.strip()

        table_specifications: list[TableSpecification] = [
            TableSpecification(
                selector=self.config.household_tiers_table_selector,
                consumer_category=ConsumerCategoryType.HOUSEHOLD,
                row_width=4,
                label_idx=0,
                metric_idx=1,
                value_idx=3,
                tariff_tier_idx=2,
            ),
            TableSpecification(
                selector=self.config.businesses_tiers_table_selector,
                consumer_category=ConsumerCategoryType.BUSINESS,
                row_width=4,
                label_idx=3,
                metric_idx=0,
                value_idx=2,
                tariff_tier_idx=1,
            ),
        ]

        electricity_tariff_tiers: list[ElectricityTariffTier] = []

        for table_specification in table_specifications:
            table_element: Tag = self.select_required(parser, table_specification.selector)
            cells: list[Tag] = table_element.select(self.config.table_cells_selector)
            rows = [
                cells[i : i + table_specification.row_width]
                for i in range(table_specification.row_width, len(cells), table_specification.row_width)
            ]
            for row in rows:
                electricity_tariff_tier: ElectricityTariffTier = self.parse(
                    row=row, specification=table_specification, valid_from_text=valid_from_text, ingested_at=ingested_at
                )
                electricity_tariff_tiers.append(electricity_tariff_tier)

        return electricity_tariff_tiers

    def parse(
        self, row: list[Tag], specification: TableSpecification, valid_from_text: str, ingested_at: datetime
    ) -> ElectricityTariffTier:
        return ElectricityTariffTier(
            ingested_at=ingested_at,
            consumer_category=specification.consumer_category,
            label=row[specification.label_idx].text.strip(),
            metric=row[specification.metric_idx].text.strip(),
            value=row[specification.value_idx].text.strip(),
            tariff_tier=row[specification.tariff_tier_idx].text.strip(),
            valid_from_text=valid_from_text,
        )
