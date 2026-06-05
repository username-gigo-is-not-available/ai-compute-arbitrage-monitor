from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.sdk import Asset, AssetAny

from infra.airflow.dags.transform_strategy import TransformStrategy
from pipeline_config import PipelineConfig

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
        transform_strategy: TransformStrategy,
    ) -> None:
        self.pipeline_configs = pipeline_configs
        self.transform_strategy = transform_strategy

    def build(self) -> DAG:
        pipeline_assets = [Asset(f"ai-compute-arbitrage-monitor/{config.dataset_name}") for config in self.pipeline_configs]

        with DAG(
                dag_id="ai-compute-arbitrage-monitor__marts",
                description="Gold layer marts — triggered by new compute offers data",
                schedule=AssetAny(*pipeline_assets),
                catchup=False,
                default_args=DEFAULT_ARGS,
                tags={"ai", "compute", "arbitrage", "monitor", "capstone"},
                max_active_runs=1,
        ) as dag:

            t_dbt_run = self.transform_strategy.build_run_operator(tag="tag:marts")
            t_dbt_test = self.transform_strategy.build_test_operator(tag="tag:marts")

            t_dbt_run >> t_dbt_test

        return dag
