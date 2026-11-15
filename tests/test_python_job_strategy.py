"""Setup and ingest run as `python -m <module> <args>`: a local command, or the python Cloud Run job (ADR-028).

Needs Airflow (the dev dependency group): skipped without it; CI runs it in the Airflow DAGs job.
"""

import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("airflow")
ROOT = Path(__file__).resolve().parents[1]
DAGS = ROOT / "infra" / "airflow" / "dags"
sys.path.insert(0, str(DAGS))

from common.enums import DatasetName
from config.execution import CloudRunConfig

from core_dag_factory import DagFactory
from pipeline_config import PipelineConfig
from python_job_strategy import CloudRunPythonJobStrategy, LocalPythonJobStrategy
from spark_job_strategy import LocalSparkJobStrategy
from transform_strategy import LocalTransformStrategy


class _FakeDbtAdapter:
    def run(self, tag: str) -> str:
        return f"dbt run --select {tag}"

    def test(self, tag: str, exclude: str | None = None) -> str:
        return f"dbt test --select {tag}"


def _dag(python_job_strategy):
    return DagFactory(
        pipeline_config=PipelineConfig(dataset_name=DatasetName.COMPUTE_OFFERS),
        python_job_strategy=python_job_strategy,
        spark_job_strategy=LocalSparkJobStrategy(),
        transform_strategy=LocalTransformStrategy(dbt_adapter=_FakeDbtAdapter()),
    ).build()


def test_local_setup_and_ingest_are_commands():
    dag = _dag(LocalPythonJobStrategy())
    assert dag.get_task("setup").bash_command == "python -m lakehouse.main compute_offers"
    assert dag.get_task("ingest").bash_command == "python -m ingest.main compute_offers {{ data_interval_end }}"


def test_cloud_run_setup_and_ingest_pass_the_module_and_args_to_the_python_job():
    dag = _dag(CloudRunPythonJobStrategy(CloudRunConfig(project_id="p", region_name="r", job_name="python-job")))
    for task_id, args in [
        ("setup", ["lakehouse.main", "compute_offers"]),
        ("ingest", ["ingest.main", "compute_offers", "{{ data_interval_end }}"]),
    ]:
        task = dag.get_task(task_id)
        assert task.job_name == "python-job"
        assert task.overrides == {"container_overrides": [{"args": args}]}


def test_the_dag_files_import_no_pipeline_code():
    # Whatever Airflow parses the DAGs (Composer included) needs none of our pipeline packages (ADR-028)
    code = (
        "import sys, core_dag; "
        "print(sorted(m for m in ('ingest', 'lakehouse', 'refine', 'pyiceberg', 'pyspark') if m in sys.modules))"
    )
    env_path = f"{ROOT / 'src'}{';' if sys.platform == 'win32' else ':'}{DAGS}"
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        cwd=ROOT,
        env={
            **__import__("os").environ,
            "PYTHONPATH": env_path,
            "SETTINGS_PATH": str(ROOT / "config" / "settings.yaml"),
        },
        check=True,
    )
    assert result.stdout.strip().splitlines()[-1] == "[]"
