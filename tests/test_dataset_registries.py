"""Every dataset is declared in every registry, or in none.

A dataset missing from one of them fails only when its task runs (KeyError in run_setup/run_ingest/run_refine).
This test catches it in CI instead. The Airflow DAG list is built from DATASETS, so it needs no check of its own.
"""

from common.enums import DatasetName
from config.datasets import DATASETS
from ingest.registry import INGEST_SPECS
from ingest.tables import BRONZE_TABLES
from refine.registry import REFINE_SPECS
from refine.tables import SILVER_TABLES


def test_every_dataset_is_declared_in_every_registry():
    every_dataset = set(DatasetName)

    assert set(DATASETS) == every_dataset
    assert set(BRONZE_TABLES) == every_dataset
    assert set(INGEST_SPECS) == every_dataset
    assert set(SILVER_TABLES) == every_dataset
    assert set(REFINE_SPECS) == every_dataset
