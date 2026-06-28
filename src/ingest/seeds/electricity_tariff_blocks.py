import logging
from dataclasses import dataclass
from datetime import datetime, UTC

from bs4 import Tag

from common.classes import Dataset
from common.enums import DatasetName, DatasetType, ConsumerCategoryType, TariffWindowType
from config.apis.evn import EVNConfig
from config.http import HttpConfig
from config.loader import ConfigLoader
from config.storage import GCPStorageConfig
from ingest.evn_base import EVNBaseIngestor
from ingest.models.electricity_tariff_block import ElectricityTariffBlock


@dataclass
class ElectricityTariffBlocksIngestor(EVNBaseIngestor):
    http_config: HttpConfig

    def load(self) -> list[ElectricityTariffBlock]:
        parser = self.fetch_soup(self.config.tariff_system_url)
        if parser is None:
            return []
        ingested_at: datetime = datetime.now(UTC)
        valid_from_text: str = parser.select_one(self.config.tariff_blocks_valid_from_selector).text.strip()

        electricity_tariff_blocks: list[ElectricityTariffBlock] = []

        paragraph_element: Tag = parser.select_one(self.config.tariff_blocks_paragraph_selector)
        rows: list[Tag] = paragraph_element.find_all(self.config.tariff_blocks_strong_selector)[:self.config.tariff_blocks_limit]
        for row in rows:
            electricity_tariff_block: ElectricityTariffBlock = self.parse(
                timestamp=ingested_at,
                data=row,
                valid_from_text=valid_from_text
            )
            if electricity_tariff_block:
                electricity_tariff_blocks.append(electricity_tariff_block)

        return electricity_tariff_blocks


    def parse(self, **kwargs) -> ElectricityTariffBlock | None:
        row: Tag = kwargs.get('data')
        valid_from_text: str = kwargs.get('valid_from_text')
        ingested_at: datetime = kwargs.get('timestamp')
        try:
            return ElectricityTariffBlock(
                ingested_at=ingested_at,
                consumer_category=ConsumerCategoryType.HOUSEHOLD,
                tariff_window=TariffWindowType.HIGH,
                block_number_text=row.text,
                kwh_boundaries_text=row.next_sibling.text,
                valid_from_text=valid_from_text,

            )
        except (KeyError, ValueError, TypeError) as e:
            self.logger.warning(f"Could not parse electricity tariff block: {row} {e}")
            return None


def main():
    loader: ConfigLoader = ConfigLoader()
    evn_config: EVNConfig = loader.get_evn()
    storage_config: GCPStorageConfig = loader.get_storage()
    electricity_tariff_blocks: Dataset = Dataset(dataset_name=DatasetName.ELECTRICITY_TARIFF_BLOCKS,
                                                 dataset_type=DatasetType.SEEDS)
    if not evn_config.enabled:
        return

    electricity_tariff_blocks_ingestor = ElectricityTariffBlocksIngestor(
        dataset=electricity_tariff_blocks,
        config=evn_config,
        storage_config=storage_config,
        http_config=loader.get_http(),
    )
    logging.info(f"Starting seed {electricity_tariff_blocks_ingestor.name}...")
    electricity_tariff_blocks_ingestor.run()


def run():
    main()


if __name__ == "__main__":
    run()
