from dataclasses import dataclass

from common.classes import Snapshot
from common.enums import DatasetName
from config.datasets import DATASETS
from config.loader import ConfigLoader
from ingest.base import Ingestor
from ingest.http_client import HttpClient
from ingest.seeds.electricity_tariff_blocks import ElectricityTariffBlocksIngestor
from ingest.seeds.electricity_tariff_fees import ElectricityTariffFeesIngestor
from ingest.seeds.electricity_tariff_tiers import ElectricityTariffTiersIngestor
from ingest.seeds.electricity_tariff_window_schedule import ElectricityTariffWindowScheduleIngestor
from ingest.sources.compute_offers import ComputeOffersIngestor
from ingest.sources.exchange_rates import ExchangeRateIngestor
from ingest.tables import BRONZE_TABLES
from ingest.write_strategy import BronzeTable


# The Bronze side of each dataset that loads data (DML): who fetches it and which table it writes. The table's
# shape is in ingest.tables; type and settings come from config.datasets.DATASETS.
@dataclass(frozen=True)
class IngestSpec:
    ingestor: type[Ingestor]
    bronze_table: BronzeTable


INGEST_SPECS: dict[DatasetName, IngestSpec] = {
    DatasetName.COMPUTE_OFFERS: IngestSpec(
        ingestor=ComputeOffersIngestor,
        bronze_table=BRONZE_TABLES[DatasetName.COMPUTE_OFFERS],
    ),
    DatasetName.EXCHANGE_RATES: IngestSpec(
        ingestor=ExchangeRateIngestor,
        bronze_table=BRONZE_TABLES[DatasetName.EXCHANGE_RATES],
    ),
    DatasetName.ELECTRICITY_TARIFF_TIERS: IngestSpec(
        ingestor=ElectricityTariffTiersIngestor,
        bronze_table=BRONZE_TABLES[DatasetName.ELECTRICITY_TARIFF_TIERS],
    ),
    DatasetName.ELECTRICITY_TARIFF_FEES: IngestSpec(
        ingestor=ElectricityTariffFeesIngestor,
        bronze_table=BRONZE_TABLES[DatasetName.ELECTRICITY_TARIFF_FEES],
    ),
    DatasetName.ELECTRICITY_TARIFF_BLOCKS: IngestSpec(
        ingestor=ElectricityTariffBlocksIngestor,
        bronze_table=BRONZE_TABLES[DatasetName.ELECTRICITY_TARIFF_BLOCKS],
    ),
    DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE: IngestSpec(
        ingestor=ElectricityTariffWindowScheduleIngestor,
        bronze_table=BRONZE_TABLES[DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE],
    ),
}


def run_ingest(dataset: str, scheduled_at: str | None = None) -> None:
    name = DatasetName(dataset)
    declaration = DATASETS[name]
    spec = INGEST_SPECS[name]
    loader = ConfigLoader()
    config = declaration.config(loader)
    if not config.enabled:
        return
    spec.ingestor(
        dataset=declaration.dataset,
        config=config,
        lakehouse_config=loader.get_lakehouse(),
        bronze_table=spec.bronze_table,
        http=HttpClient(loader.get_http()),
        snapshot=Snapshot.from_scheduled_at(scheduled_at),
    ).run()
