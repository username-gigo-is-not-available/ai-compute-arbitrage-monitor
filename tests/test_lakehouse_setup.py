"""lakehouse.setup: the DDL step that makes one dataset's Bronze and Silver tables match their declarations (ADR-022)."""

import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from common.enums import DatasetName, DataStageType
from config.datasets import DATASETS
from config.lakehouse import GCPLakehouseConfig
from ingest.tables import BRONZE_TABLES
from lakehouse.setup import run_setup
from refine.tables import SILVER_TABLES

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"


@pytest.fixture
def lakehouse(monkeypatch, catalog):
    """run_setup against the fake catalog instead of BigLake."""
    monkeypatch.setenv("GCP_PROJECT_ID", "test")
    monkeypatch.setattr(GCPLakehouseConfig, "open_catalog", lambda self: catalog)
    return catalog


def _columns(catalog, stage: DataStageType, name: DatasetName) -> list[str]:
    table_id = GCPLakehouseConfig.stage_namespace(stage, DATASETS[name].dataset_type), name.value
    return [f.name for f in catalog.load_table(table_id).schema().fields]


@pytest.mark.parametrize("name", list(DatasetName), ids=lambda name: name.value)
def test_setup_creates_both_tables_as_declared(lakehouse, name):
    run_setup(name.value)

    assert lakehouse.created_tables == [
        (GCPLakehouseConfig.stage_namespace(DataStageType.BRONZE, DATASETS[name].dataset_type), name.value),
        (GCPLakehouseConfig.stage_namespace(DataStageType.SILVER, DATASETS[name].dataset_type), name.value),
    ]
    assert _columns(lakehouse, DataStageType.BRONZE, name) == [f.name for f in BRONZE_TABLES[name].schema.fields]
    assert _columns(lakehouse, DataStageType.SILVER, name) == [
        f.name for f in SILVER_TABLES[name].iceberg_schema().fields
    ]


def test_setup_of_a_disabled_dataset_touches_nothing(lakehouse, monkeypatch, tmp_path):
    settings = yaml.safe_load((REPO_ROOT / "config" / "settings.yaml").read_text(encoding="utf-8"))
    settings["apis"]["exchange_rate"]["enabled"] = False
    path = tmp_path / "settings.yaml"
    path.write_text(yaml.safe_dump(settings), encoding="utf-8")
    monkeypatch.setenv("SETTINGS_PATH", str(path))

    run_setup(DatasetName.EXCHANGE_RATES.value)

    assert lakehouse.namespaces == set()
    assert lakehouse.created_tables == []


def test_setup_does_not_import_the_ingestors_or_their_libraries():
    # It runs on Dataproc, whose image has pyspark, pyiceberg and the catalog auth libraries but not the ingestors'
    # HTTP and parsing libraries. A fresh interpreter shows what importing it really loads.
    script = (
        "import sys, lakehouse.setup; "
        # not tenacity: pyiceberg's REST catalog depends on it, so it comes with pyiceberg
        "print(','.join(m for m in ('ingest.base', 'ingest.registry', 'bs4', 'vastai', 'vastai_sdk') "
        "if m in sys.modules))"
    )
    env = {**os.environ, "PYTHONPATH": str(SRC)}

    result = subprocess.run([sys.executable, "-c", script], env=env, capture_output=True, text=True, check=True)

    assert result.stdout.strip() == ""
