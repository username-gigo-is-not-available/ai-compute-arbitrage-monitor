"""EVN seed ingestors turn the tariff pages into Bronze records (load + parse).

The pages in tests/fixtures are copies of the live EVN pages (2026-10-21, scripts and styles stripped), read with the
real selectors from settings.yaml. No network.
"""

import re
from pathlib import Path

from fakes import FakeResponse, build_ingestor

from common.enums import ConsumerCategoryType, DatasetName, DatasetType
from config.loader import ConfigLoader
from ingest.seeds.electricity_tariff_blocks import ElectricityTariffBlocksIngestor
from ingest.seeds.electricity_tariff_fees import ElectricityTariffFeesIngestor
from ingest.seeds.electricity_tariff_tiers import ElectricityTariffTiersIngestor
from ingest.seeds.electricity_tariff_window_schedule import ElectricityTariffWindowScheduleIngestor

FIXTURES = Path(__file__).parent / "fixtures"


CONFIG = ConfigLoader().get_evn()
PAGES = {
    CONFIG.tariff_tiers_url: FIXTURES / "evn_invoices.html",
    CONFIG.tariff_system_url: FIXTURES / "evn_tariff_system.html",
}


def _load(ingestor_class, name: DatasetName) -> list:
    def send(url: str, **kwargs) -> FakeResponse:
        return FakeResponse(text=PAGES[url].read_text(encoding="utf-8"))

    return build_ingestor(ingestor_class, name, DatasetType.SEEDS, CONFIG, send).load()


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
