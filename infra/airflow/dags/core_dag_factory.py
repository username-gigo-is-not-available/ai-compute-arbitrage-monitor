from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset

from callable_builder import CallableBuilder
from transform_strategy import TransformStrategy
from pipeline_config import PipelineConfig
from refine_strategy import RefineStrategy

DEFAULT_ARGS = {
    "owner": "gigo",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


class DagFactory:

    def __init__(self, pipeline_config: PipelineConfig,
                 refine_strategy: RefineStrategy,
                 transform_strategy: TransformStrategy
                 ) -> None:
        self.pipeline_config = pipeline_config
        self.refine_strategy = refine_strategy
        self.transform_strategy = transform_strategy

    def build(self) -> DAG:
        config: PipelineConfig = self.pipeline_config

        with DAG(
                dag_id=config.dag_id,
                description=config.description,
                schedule=config.schedule,
                start_date=config.start_date,
                catchup=False,
                default_args=DEFAULT_ARGS,
                tags={"ai", "compute", "arbitrage", "monitor", "capstone"},
                max_active_runs=1,
        ) as dag:
            t_ingest = PythonOperator(
                task_id="ingest",
                python_callable=CallableBuilder(config.ingest_module).build(),
            )

            t_refine = self.refine_strategy.build_operator(self.pipeline_config)

            t_ext_table = self.transform_strategy.build_run_operation_operator(
                operation_name="stage_external_sources",
                tag=config.external_table_selector,
            )

            t_dbt_run = self.transform_strategy.build_run_operator(tag=config.dbt_tag)

            t_dbt_test = self.transform_strategy.build_test_operator(tag=config.dbt_tag)
            t_dbt_test.outlets = [Asset(f"ai-compute-arbitrage-monitor/{config.dataset_name}")]

            t_ingest >> t_refine >> t_ext_table >> t_dbt_run >> t_dbt_test

        return dag
