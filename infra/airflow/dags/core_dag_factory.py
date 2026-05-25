from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset

from callable_builder import CallableBuilder
from pipeline_config import PipelineConfig
from refine_strategy import RefineStrategy
from transform_adapter import DbtAdapter

DEFAULT_ARGS = {
    "owner": "gigo",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


class DagFactory:

    def __init__(self, pipeline_config: PipelineConfig,
                 refine_strategy: RefineStrategy,
                 dbt: DbtAdapter
                 ) -> None:
        self.pipeline_config = pipeline_config
        self.refine_strategy = refine_strategy
        self.dbt = dbt

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

            t_ext_table = BashOperator(
                task_id="register_external_tables",
                bash_command=self.dbt.run_operation(
                    "stage_external_sources",
                    config.external_table_selector,
                ),
            )

            t_dbt_run = BashOperator(
                task_id="process",
                bash_command=self.dbt.run(config.dbt_tag),
            )

            t_dbt_test = BashOperator(
                task_id="test",
                bash_command=self.dbt.test(config.dbt_tag),
                outlets=[Asset(f"ai-compute-arbitrage-monitor/{config.dataset_name}")],
            )

            t_ingest >> t_refine >> t_ext_table >> t_dbt_run >> t_dbt_test

        return dag
