"""ExchangeRateIngestor turns the API's JSON into Bronze rows."""

import pytest
from fakes import FakeResponse, build_ingestor
from pyiceberg.schema import Schema

from common.enums import DatasetName, DatasetType, DataStageType
from config.apis.exchange_rate import ExchangeRateConfig
from config.lakehouse import GCPLakehouseConfig
from ingest.sources.exchange_rates import ExchangeRateIngestor
from ingest.tables import BRONZE_TABLES
from lakehouse.sync import sync_table

CONFIG = ExchangeRateConfig(
    enabled=True, base_url="https://exchangerate.test/", from_currency="USD", to_currency="MKD", api_key="test-key"
)
RESPONSE = {"conversion_rates": {"MKD": 58.15}, "time_last_update_utc": "Tue, 03 Sep 2026 00:00:00 +0000"}


def _ingestor() -> ExchangeRateIngestor:
    return build_ingestor(
        ExchangeRateIngestor,
        DatasetName.EXCHANGE_RATES,
        DatasetType.SOURCES,
        CONFIG,
        lambda url, **kwargs: FakeResponse(json_data=RESPONSE),
    )


def test_run_appends_the_rate_to_bronze(bronze):
    rows = bronze(_ingestor()).to_pylist()
    assert [(r["from_currency"], r["to_currency"], r["value"]) for r in rows] == [("USD", "MKD", 58.15)]


def test_records_with_a_column_the_table_lacks_fail_and_write_nothing(monkeypatch, catalog):
    # A field added to the record model but not to the Bronze schema must not be dropped quietly (#41)
    monkeypatch.setattr(GCPLakehouseConfig, "open_catalog", lambda self: catalog)
    declared = BRONZE_TABLES[DatasetName.EXCHANGE_RATES]
    without_value = Schema(*[f for f in declared.schema.fields if f.name != "value"])
    ingestor = _ingestor()
    table_id = ingestor.lakehouse_config.table_id(DataStageType.BRONZE, ingestor.dataset)
    sync_table(
        catalog, table_id, schema=without_value, partition_spec=declared.write_strategy.partition_spec(without_value)
    )
    ingestor.bronze_table = declared

    with pytest.raises(ValueError, match=r"\['value'\]"):
        ingestor.run()

    assert catalog.load_table(table_id).current_snapshot() is None
