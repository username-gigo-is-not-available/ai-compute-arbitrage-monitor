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
from ingest.models.electricity_tariff_fee import ElectricityTariffFee


@dataclass(frozen=True)
class CellSpecification:
    consumer_category: ConsumerCategoryType
    label_idx: int
    metric_idx: int
    value_idx: int


@dataclass
class ElectricityTariffFeesIngestor(EVNBaseIngestor):
    http_config: HttpConfig

    def load(self) -> list[ElectricityTariffFee]:
        parser = self.fetch_soup(self.config.tariff_tiers_url)
        if parser is None:
            return []
        ingested_at: datetime = datetime.now(UTC)
        valid_from_text: str = self.get_valid_from_text(parser)

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
            electricity_tariff_fee: ElectricityTariffFee = self.parse(data=cells,
                                                                      timestamp=ingested_at,
                                                                      specification=cell_specification,
                                                                      valid_from_text=valid_from_text)
            if electricity_tariff_fee:
                electricity_tariff_fees.append(electricity_tariff_fee)

        return electricity_tariff_fees

    def parse(self, **kwargs) -> ElectricityTariffFee | None:
        row: list[Tag] = kwargs.get('data')
        specification: CellSpecification = kwargs.get('specification')
        valid_from_text: str = kwargs.get('valid_from_text')
        ingested_at: datetime = kwargs.get('timestamp')
        try:
            return ElectricityTariffFee(
                ingested_at=ingested_at,
                consumer_category=specification.consumer_category,
                label=row[specification.label_idx].text.strip(),
                metric=row[specification.metric_idx].text.strip(),
                value=row[specification.value_idx].text.strip(),
                valid_from_text=valid_from_text
            )
        except (KeyError, ValueError, TypeError) as e:
            self.logger.warning(f"Could not parse electricity tariff fee: {row} {e}")
            return None


def main():
    loader: ConfigLoader = ConfigLoader()
    evn_config: EVNConfig = loader.get_evn()
    storage_config: GCPStorageConfig = loader.get_storage()
    electricity_tariff_fees: Dataset = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_FEES,
                                               dataset_type=DatasetType.SEEDS)
    if not evn_config.enabled:
        return

    electricity_tariff_fees_ingestor = ElectricityTariffFeesIngestor(
        dataset=electricity_tariff_fees,
        config=evn_config,
        storage_config=storage_config,
        http_config=loader.get_http(),
    )
    logging.info(f"Starting seed {electricity_tariff_fees_ingestor.name}...")
    electricity_tariff_fees_ingestor.run()


def run():
    main()


if __name__ == "__main__":
    run()
