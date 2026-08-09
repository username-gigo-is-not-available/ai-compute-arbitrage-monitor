import logging
from dataclasses import dataclass
from datetime import datetime, UTC

from bs4 import Tag

from common.classes import Dataset
from common.enums import DatasetName, DatasetType, ConsumerCategoryType
from config.apis.evn import EVNConfig
from config.http import HttpConfig
from config.loader import ConfigLoader
from config.storage import GCPStorageConfig
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
class ElectricityTariffTiersIngestor(EVNBaseIngestor):
    http_config: HttpConfig

    def load(self) -> list[ElectricityTariffTier]:
        parser = self.fetch_soup(self.config.tariff_tiers_url)
        if parser is None:
            return []
        ingested_at: datetime = datetime.now(UTC)
        valid_from_text: str = parser.select_one(self.config.tariff_tiers_valid_from_text_selector).text.strip()

        table_specifications: list[TableSpecification] = [
            TableSpecification(selector=self.config.household_tiers_table_selector,
                               consumer_category=ConsumerCategoryType.HOUSEHOLD,
                               row_width=4,
                               label_idx=0,
                               metric_idx=1,
                               value_idx=3,
                               tariff_tier_idx=2,
                               ),
            TableSpecification(selector=self.config.businesses_tiers_table_selector,
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
            table_element: Tag = parser.select_one(table_specification.selector)
            cells: list[Tag] = table_element.select(self.config.table_cells_selector)
            rows = [cells[i:i + table_specification.row_width] for i in
                    range(table_specification.row_width, len(cells), table_specification.row_width)]
            for row in rows:
                electricity_tariff_tier: ElectricityTariffTier = self.parse(data=row,
                                                                            timestamp=ingested_at,
                                                                            specification=table_specification,
                                                                            valid_from_text=valid_from_text)
                if electricity_tariff_tier:
                    electricity_tariff_tiers.append(electricity_tariff_tier)

        return electricity_tariff_tiers

    def parse(self, **kwargs) -> ElectricityTariffTier | None:
        row: list[Tag] = kwargs.get('data')
        specification: TableSpecification = kwargs.get('specification')
        valid_from_text: str = kwargs.get('valid_from_text')
        ingested_at: datetime = kwargs.get('timestamp')
        try:
            return ElectricityTariffTier(
                ingested_at=ingested_at,
                consumer_category=specification.consumer_category,
                label=row[specification.label_idx].text.strip(),
                metric=row[specification.metric_idx].text.strip(),
                value=row[specification.value_idx].text.strip(),
                tariff_tier=row[specification.tariff_tier_idx].text.strip(),
                valid_from_text=valid_from_text
            )
        except (KeyError, ValueError, TypeError) as e:
            self.logger.warning(f"Could not parse electricity tariff tier: {row} {e}")
            return None


def main():
    loader: ConfigLoader = ConfigLoader()
    evn_config: EVNConfig = loader.get_evn()
    storage_config: GCPStorageConfig = loader.get_storage()
    electricity_tariff_tiers: Dataset = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_TIERS,
                                                dataset_type=DatasetType.SEEDS)
    if not evn_config.enabled:
        return

    electricity_tariff_tiers_ingestor = ElectricityTariffTiersIngestor(
        dataset=electricity_tariff_tiers,
        config=evn_config,
        storage_config=storage_config,
        http_config=loader.get_http(),
    )
    logging.info(f"Starting seed {electricity_tariff_tiers_ingestor.name}...")
    electricity_tariff_tiers_ingestor.run()


def run():
    main()


if __name__ == "__main__":
    run()
