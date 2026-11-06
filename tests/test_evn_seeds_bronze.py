"""EVN seed ingestors turn the tariff pages into Bronze records (load + parse).

The pages in tests/fixtures are copies of the live EVN pages (2026-10-21, scripts and styles stripped), read with the
real selectors from settings.yaml. No network.
"""

import re
from pathlib import Path

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType

from common.classes import Dataset, Snapshot
from common.enums import ConsumerCategoryType, DatasetName, DatasetType
from config.http import HttpConfig
from config.lakehouse import GCPLakehouseConfig
from config.loader import ConfigLoader
from ingest.http_client import HttpClient
from ingest.seeds.electricity_tariff_blocks import ElectricityTariffBlocksIngestor
from ingest.seeds.electricity_tariff_fees import ElectricityTariffFeesIngestor
from ingest.seeds.electricity_tariff_tiers import ElectricityTariffTiersIngestor
from ingest.seeds.electricity_tariff_window_schedule import ElectricityTariffWindowScheduleIngestor
from ingest.write_strategy import AppendByDay, BronzeTable

FIXTURES = Path(__file__).parent / "fixtures"


class FakePage:
    def __init__(self, path: Path) -> None:
        self.status_code = 200
        self.headers = {}
        self.text = path.read_text(encoding="utf-8")

    def raise_for_status(self) -> None:
        pass


CONFIG = ConfigLoader().get_evn()
PAGES = {
    CONFIG.tariff_tiers_url: FIXTURES / "evn_invoices.html",
    CONFIG.tariff_system_url: FIXTURES / "evn_tariff_system.html",
}


def _load(ingestor_class, name: DatasetName) -> list:
    ingestor = ingestor_class(
        dataset=Dataset(dataset_name=name, dataset_type=DatasetType.SEEDS),
        config=CONFIG,
        lakehouse_config=GCPLakehouseConfig(catalog_id="test", project_id="test", warehouse="gs://test"),
        bronze_table=BronzeTable(
            Schema(NestedField(field_id=1, name="id", field_type=StringType(), required=False)),
            AppendByDay(column="ingested_at"),
        ),
        http=HttpClient(
            HttpConfig(timeout_seconds=30, retry_count=1, retry_delay_seconds=0),
            send=lambda url, **kwargs: FakePage(PAGES[url]),
        ),
        snapshot=Snapshot.from_scheduled_at(None),
    )
    return ingestor.load()


def test_tiers_read_both_consumer_tables():
    tiers = _load(ElectricityTariffTiersIngestor, DatasetName.ELECTRICITY_TARIFF_TIERS)
    assert len(tiers) == 7
    assert {t.consumer_category for t in tiers} == {ConsumerCategoryType.HOUSEHOLD, ConsumerCategoryType.BUSINESS}
    assert all(t.value for t in tiers), "every tier has a price"


def test_fees_read_four_cells():
    fees = _load(ElectricityTariffFeesIngestor, DatasetName.ELECTRICITY_TARIFF_FEES)
    assert len(fees) == 4
    assert sorted(f.consumer_category for f in fees) == sorted(
        [ConsumerCategoryType.HOUSEHOLD, ConsumerCategoryType.BUSINESS] * 2
    )


def test_blocks_read_each_block_with_its_kwh_bounds():
    blocks = _load(ElectricityTariffBlocksIngestor, DatasetName.ELECTRICITY_TARIFF_BLOCKS)
    assert len(blocks) == 4
    assert all(b.kwh_boundaries_text for b in blocks)


def test_schedule_is_one_record():
    schedules = _load(ElectricityTariffWindowScheduleIngestor, DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE)
    assert len(schedules) == 1
    assert schedules[0].schedule_text


def test_every_record_of_one_fetch_shares_ingested_at_and_valid_from_text():
    # Silver keeps the latest fetch per valid_from by ingested_at, so one fetch must carry one timestamp.
    tiers = _load(ElectricityTariffTiersIngestor, DatasetName.ELECTRICITY_TARIFF_TIERS)
    assert len({t.ingested_at for t in tiers}) == 1
    assert len({t.valid_from_text for t in tiers}) == 1
    assert re.search(r"\d{2}\.\d{2}\.\d{4}", tiers[0].valid_from_text)
