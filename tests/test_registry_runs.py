"""run_ingest and run_refine: the task entry points that build a dataset's ingestor or pipeline from settings.yaml."""

from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml

from common.enums import DatasetName
from config.datasets import DATASETS
from ingest.registry import INGEST_SPECS, run_ingest
from ingest.tables import BRONZE_TABLES
from refine.registry import REFINE_SPECS, run_refine
from refine.tables import SILVER_TABLES

SETTINGS = Path(__file__).resolve().parents[1] / "config" / "settings.yaml"


@pytest.fixture
def ran(monkeypatch) -> list:
    """The ingestors and pipelines whose run() was called; nothing is fetched or written."""
    monkeypatch.setenv("GCP_PROJECT_ID", "test")
    started: list = []
    for ingest in INGEST_SPECS.values():
        monkeypatch.setattr(ingest.ingestor, "run", lambda self: started.append(self))
    for refine in REFINE_SPECS.values():
        monkeypatch.setattr(refine.pipeline, "run", lambda self: started.append(self))
    return started


@pytest.fixture
def exchange_rates_disabled(monkeypatch, tmp_path) -> None:
    settings = yaml.safe_load(SETTINGS.read_text(encoding="utf-8"))
    settings["apis"]["exchange_rate"]["enabled"] = False
    path = tmp_path / "settings.yaml"
    path.write_text(yaml.safe_dump(settings), encoding="utf-8")
    monkeypatch.setenv("SETTINGS_PATH", str(path))


@pytest.mark.parametrize("name", list(DatasetName), ids=lambda name: name.value)
def test_ingest_runs_the_dataset_ingestor_for_the_scheduled_day(ran, name):
    run_ingest(name.value, "2026-11-09T06:00:00+00:00")

    [ingestor] = ran
    assert type(ingestor) is INGEST_SPECS[name].ingestor
    assert ingestor.dataset == DATASETS[name].dataset
    assert ingestor.bronze_table is BRONZE_TABLES[name]
    assert ingestor.snapshot.at == datetime(2026, 11, 9, tzinfo=UTC)


@pytest.mark.parametrize("name", list(DatasetName), ids=lambda name: name.value)
def test_refine_runs_the_dataset_pipeline(ran, monkeypatch, spark, name):
    monkeypatch.setattr("refine.registry.initialize_spark", lambda: spark)

    run_refine(name.value)

    [pipeline] = ran
    assert type(pipeline) is REFINE_SPECS[name].pipeline
    assert pipeline.dataset == DATASETS[name].dataset
    assert pipeline.silver_table is SILVER_TABLES[name]


def test_a_disabled_dataset_runs_nothing(ran, exchange_rates_disabled):
    run_ingest(DatasetName.EXCHANGE_RATES.value)
    run_refine(DatasetName.EXCHANGE_RATES.value)

    assert ran == []
