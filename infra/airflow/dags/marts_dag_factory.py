from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.sdk import Asset, AssetAny
from airflow.providers.standard.operators.bash import BashOperator

from infra.airflow.dags.pipeline_config import PipelineConfig

DEFAULT_ARGS = {
    "owner": "gigo",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}

class MartsDagFactory:

    def __init__(
        self,
        pipeline_configs: list[PipelineConfig],
        dbt_project_dir: str,
        dbt_target_dir: str,
    ) -> None:
        self.pipeline_configs = pipeline_configs
        self.dbt_directory = dbt_project_dir
        self.dbt_target_dir = dbt_target_dir

    def dbt_run(self, tag: str) -> str:
        return (
            f"/opt/airflow/.venv/bin/dbt run"
            f" --project-dir {self.dbt_directory}"
            f" --profiles-dir {self.dbt_directory}"
            f" --target-path {self.dbt_target_dir}"
            f" --select {tag}"
        )

    def dbt_test(self, tag: str) -> str:
        return (
            f"/opt/airflow/.venv/bin/dbt test"
            f" --project-dir {self.dbt_directory}"
            f" --profiles-dir {self.dbt_directory}"
            f" --target-path {self.dbt_target_dir}"
            f" --select {tag}"
        )

    def build(self) -> DAG:
        pipeline_assets = [Asset(f"gpu_arbitrage/{cfg.name}") for cfg in self.pipeline_configs]

        with DAG(
                dag_id="gpu_arbitrage__marts",
                description="Gold layer marts — triggered by new compute offers data",
                schedule=AssetAny(*pipeline_assets),
                catchup=False,
                default_args=DEFAULT_ARGS,
                tags={"gpu-arbitrage", "capstone"},
                max_active_runs=1,
        ) as dag:

            t_dbt_run = BashOperator(
                task_id="run",
                bash_command=self.dbt_run("tag:marts"),
            )

            t_dbt_test = BashOperator(
                task_id="test",
                bash_command=self.dbt_test("tag:marts"),
            )
            t_dbt_run >> t_dbt_test

        return dag
