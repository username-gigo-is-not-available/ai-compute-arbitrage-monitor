"""Setup and refine both run a package's main.py with the dataset name, locally and on Dataproc.

Needs Airflow (the dev dependency group): skipped without it; CI runs it in the Airflow DAGs job.
"""

import re
import sys
from pathlib import Path
from types import SimpleNamespace

import jinja2
import pytest

pytest.importorskip("airflow")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "infra" / "airflow" / "dags"))

from common.enums import DatasetName
from config.bucket import GCSBucketConfig
from config.cluster import GCPClusterConfig
from config.versions import DATAPROC_RUNTIME_VERSION, DATAPROC_SPARK_PACKAGES

from pipeline_config import PipelineConfig
from spark_job_strategy import DataprocSparkJobStrategy, LocalSparkJobStrategy

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


@pytest.mark.parametrize("package", ["refine"])
def test_dataproc_runs_the_package_entry_script_with_the_dataset_name(package):
    batch = DATAPROC.batch_config(CONFIG, package)["pyspark_batch"]

    assert batch["main_python_file_uri"] == f"gs://bucket/jobs/{package}/main.py"
    assert batch["args"] == ["electricity_tariff_fees"]


def test_dataproc_batch_gets_the_iceberg_jars_at_launch_on_the_pinned_runtime():
    runtime = DATAPROC.batch_config(CONFIG, "refine")["runtime_config"]

    assert runtime["version"] == DATAPROC_RUNTIME_VERSION
    assert runtime["properties"]["spark.jars.packages"] == ",".join(DATAPROC_SPARK_PACKAGES)


def test_the_dataproc_image_is_built_on_the_pinned_runtime():
    # The jars are chosen for the runtime; an image on another runtime would not match them (#124)
    dockerfile = Path(__file__).resolve().parents[1] / "infra" / "docker" / "Dockerfile.dataproc"
    base = next(line for line in dockerfile.read_text(encoding="utf-8").splitlines() if line.startswith("FROM "))

    assert base.endswith(f"/dataproc_{DATAPROC_RUNTIME_VERSION}")


def test_setup_and_refine_get_different_batch_ids_in_the_same_run():
    setup = DataprocSparkJobStrategy.generate_batch_id(CONFIG, "setup")
    refine = DataprocSparkJobStrategy.generate_batch_id(CONFIG, "refine")

    assert setup == "setup-electricity-tariff-fees-{{ ts_nodash | lower }}-{{ ti.try_number }}"
    assert setup != refine


def render_batch_id(dataset_name: DatasetName, try_number: int) -> str:
    template = DataprocSparkJobStrategy.generate_batch_id(PipelineConfig(dataset_name=dataset_name), "refine")
    return jinja2.Template(template).render(ts_nodash="20261126T000000", ti=SimpleNamespace(try_number=try_number))


def test_a_retry_gets_its_own_batch_id():
    first = render_batch_id(DatasetName.COMPUTE_OFFERS, try_number=1)
    retry = render_batch_id(DatasetName.COMPUTE_OFFERS, try_number=2)

    assert first == "refine-compute-offers-20261126t000000-1"
    assert retry == "refine-compute-offers-20261126t000000-2"


# Dataproc batch ids: lowercase letters, digits and hyphens, 4-63 characters
@pytest.mark.parametrize("dataset_name", list(DatasetName))
def test_batch_id_is_valid_for_dataproc(dataset_name):
    batch_id = render_batch_id(dataset_name, try_number=10)

    assert re.fullmatch(r"[a-z0-9-]{4,63}", batch_id), batch_id


@pytest.mark.parametrize("package", ["refine"])
def test_local_runs_the_same_entry_script_with_the_dataset_name(package):
    assert LocalSparkJobStrategy.command(CONFIG, package) == f"python -m {package}.main electricity_tariff_fees"
