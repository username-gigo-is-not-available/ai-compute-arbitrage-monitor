"""Every file in the DAG folder imports the way the Airflow scheduler loads it, and builds the expected DAGs."""

import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")

from airflow.models.dagbag import DagBag  # noqa: E402

DAG_FOLDER = Path(__file__).resolve().parents[1] / "infra" / "airflow" / "dags"
sys.path.insert(0, str(DAG_FOLDER))

from pipeline_config import PIPELINE_CONFIGS  # noqa: E402


# The import timeout uses SIGALRM, which Windows lacks; without it every file fails and import_errors stays empty.
@pytest.fixture(scope="module")
def dag_bag() -> DagBag:
    with pytest.MonkeyPatch.context() as mp:
        mp.setenv("AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT", "0")
        return DagBag(dag_folder=str(DAG_FOLDER), include_examples=False)


def test_no_import_errors(dag_bag: DagBag) -> None:
    assert dag_bag.import_errors == {}


def test_every_dataset_has_a_dag(dag_bag: DagBag) -> None:
    assert {c.dag_id for c in PIPELINE_CONFIGS} <= set(dag_bag.dag_ids)
