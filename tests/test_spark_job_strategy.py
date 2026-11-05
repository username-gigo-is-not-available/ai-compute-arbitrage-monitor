"""Setup and refine both run a package's main.py with the dataset name, locally and on Dataproc.

Needs Airflow (the dev dependency group): skipped without it; CI runs it in the Airflow DAGs job.
"""

import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "infra" / "airflow" / "dags"))

from common.enums import DatasetName  # noqa: E402
from config.bucket import GCSBucketConfig  # noqa: E402
from config.cluster import GCPClusterConfig  # noqa: E402

from pipeline_config import PipelineConfig  # noqa: E402
from spark_job_strategy import DataprocSparkJobStrategy, LocalSparkJobStrategy  # noqa: E402

CONFIG = PipelineConfig(dataset_name=DatasetName.ELECTRICITY_TARIFF_FEES)
DATAPROC = DataprocSparkJobStrategy(
    cluster_config=GCPClusterConfig(
        project_id="p",
        region_name="r",
        image_tag="i",
        runtime_packages=[],
        subnetwork_name="s",
        service_account_email="sa",
    ),
    bucket_config=GCSBucketConfig(bucket_name="bucket"),
)


@pytest.mark.parametrize("package", ["lakehouse", "refine"])
def test_dataproc_runs_the_package_entry_script_with_the_dataset_name(package):
    batch = DATAPROC.batch_config(CONFIG, package)["pyspark_batch"]

    assert batch["main_python_file_uri"] == f"gs://bucket/jobs/{package}/main.py"
    assert batch["args"] == ["electricity_tariff_fees"]


def test_setup_and_refine_get_different_batch_ids_in_the_same_run():
    setup = DataprocSparkJobStrategy.generate_batch_id(CONFIG, "setup")
    refine = DataprocSparkJobStrategy.generate_batch_id(CONFIG, "refine")

    assert setup == "setup-electricity-tariff-fees-{{ ts_nodash | lower }}"
    assert setup != refine


@pytest.mark.parametrize("package", ["lakehouse", "refine"])
def test_local_runs_the_same_entry_script_with_the_dataset_name(package):
    assert LocalSparkJobStrategy.command(CONFIG, package) == f"python -m {package}.main electricity_tariff_fees"
