from __future__ import annotations

from airflow import DAG
from airflow.sdk import Asset, AssetAny

from pipeline_config import DAG_TAGS, DEFAULT_ARGS, PipelineConfig
from transform_strategy import TransformStrategy


class MartsDagFactory:
    def __init__(
        self,
        pipeline_configs: list[PipelineConfig],
        transform_strategy: TransformStrategy,
    ) -> None:
        self.pipeline_configs = pipeline_configs
        self.transform_strategy = transform_strategy

    def build(self) -> DAG:
        pipeline_assets = [Asset(config.asset_name) for config in self.pipeline_configs]

        with DAG(
            dag_id="ai-compute-arbitrage-monitor__marts",
            description="Gold layer marts — triggered when any dataset's DAG passes its tests",
            schedule=AssetAny(*pipeline_assets),
            catchup=False,
            default_args=DEFAULT_ARGS,
            tags=DAG_TAGS,
            max_active_runs=1,
        ) as dag:
            t_dbt_run = self.transform_strategy.build_run_operator(tag="tag:marts")
            t_dbt_test = self.transform_strategy.build_test_operator(tag="tag:marts")

            t_dbt_run >> t_dbt_test

        return dag
