from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.sdk import Asset, AssetAny
from airflow.providers.standard.operators.bash import BashOperator

from pipeline_config import PipelineConfig
from transform_adapter import DbtAdapter

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
        dbt: DbtAdapter
    ) -> None:
        self.pipeline_configs = pipeline_configs
        self.dbt = dbt

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
                bash_command=self.dbt.run("tag:marts"),
            )

            t_dbt_test = BashOperator(
                task_id="test",
                bash_command=self.dbt.test("tag:marts"),
            )
            t_dbt_run >> t_dbt_test

        return dag
