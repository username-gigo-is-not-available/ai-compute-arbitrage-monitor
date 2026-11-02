"""Every dataset is declared in all four places, or in none.

A dataset missing from one of them fails only when its task runs (KeyError in run_ingest/run_refine, or no DAG at
all). This test catches it in CI instead. pipeline_config.py imports no Airflow, so it runs there too.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "infra" / "airflow" / "dags"))

from common.enums import DatasetName  # noqa: E402
from config.datasets import DATASETS  # noqa: E402
from ingest.registry import INGEST_SPECS  # noqa: E402
from pipeline_config import PIPELINE_CONFIGS  # noqa: E402
from refine.registry import REFINE_SPECS  # noqa: E402


def test_every_dataset_is_declared_in_all_four_places():
    every_dataset = set(DatasetName)

    assert set(DATASETS) == every_dataset
    assert set(INGEST_SPECS) == every_dataset
    assert set(REFINE_SPECS) == every_dataset
    assert {config.dataset_name for config in PIPELINE_CONFIGS} == every_dataset


def test_each_dataset_has_one_dag():
    names = [config.dataset_name for config in PIPELINE_CONFIGS]

    assert len(names) == len(set(names))
