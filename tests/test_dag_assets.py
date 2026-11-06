"""The marts DAG waits on exactly the assets the dataset DAGs emit, so a renamed asset cannot silently stop it.

Needs Airflow (the dev dependency group): skipped without it; CI runs it in the Airflow DAGs job.
"""

import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "infra" / "airflow" / "dags"))

from core_dag_factory import DagFactory
from marts_dag_factory import MartsDagFactory
from pipeline_config import PIPELINE_CONFIGS
from spark_job_strategy import LocalSparkJobStrategy
from transform_strategy import LocalTransformStrategy


class _FakeDbtAdapter:
    def run(self, tag: str) -> str:
        return f"dbt run --select {tag}"

    def test(self, tag: str) -> str:
        return f"dbt test --select {tag}"


TRANSFORM = LocalTransformStrategy(dbt_adapter=_FakeDbtAdapter())


def test_marts_dag_waits_on_exactly_the_assets_the_dataset_dags_emit():
    emitted = set()
    for config in PIPELINE_CONFIGS:
        dag = DagFactory(
            pipeline_config=config, spark_job_strategy=LocalSparkJobStrategy(), transform_strategy=TRANSFORM
        ).build()
        emitted |= {asset.name for task in dag.tasks for asset in task.outlets}

    marts = MartsDagFactory(pipeline_configs=PIPELINE_CONFIGS, transform_strategy=TRANSFORM).build()
    awaited = {asset.name for _, asset in marts.timetable.asset_condition.iter_assets()}

    assert emitted == awaited
    assert len(emitted) == len(PIPELINE_CONFIGS)
