"""Local and Dataproc both start one entry script, refine/main.py, and pass it the dataset name.

Needs Airflow, which CI does not install (the dev dependency group), so it is skipped there and runs locally:
    uv run pytest tests/test_refine_strategy.py
"""

import sys
from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("airflow")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "infra" / "airflow" / "dags"))

from common.enums import DatasetName  # noqa: E402
from config.bucket import GCSBucketConfig  # noqa: E402
from config.cluster import GCPClusterConfig  # noqa: E402
from pipeline_config import PipelineConfig  # noqa: E402
from refine_strategy import DataprocRefineStrategy, LocalRefineStrategy  # noqa: E402

CONFIG = PipelineConfig(dataset_name=DatasetName.ELECTRICITY_TARIFF_FEES, schedule="@daily",
                        start_date=datetime(2026, 1, 1))


def test_dataproc_runs_the_one_entry_script_with_the_dataset_name():
    strategy = DataprocRefineStrategy(
        cluster_config=GCPClusterConfig(project_id="p", region_name="r", image_tag="i", runtime_packages=[],
                                        subnetwork_name="s", service_account_email="sa"),
        bucket_config=GCSBucketConfig(bucket_name="bucket"),
    )

    batch = strategy.batch_config(CONFIG)["pyspark_batch"]

    assert batch["main_python_file_uri"] == "gs://bucket/jobs/refine/main.py"
    assert batch["args"] == ["electricity_tariff_fees"]


def test_local_runs_the_same_entry_script_with_the_dataset_name():
    assert LocalRefineStrategy.command(CONFIG) == "python -m refine.main electricity_tariff_fees"
