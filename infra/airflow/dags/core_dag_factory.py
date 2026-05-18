from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset

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
        cfg = self.pipeline_config

        with DAG(
                dag_id=cfg.dag_id,
                description=cfg.description,
                schedule=cfg.schedule,
                start_date=cfg.start_date,
                catchup=False,
                default_args=DEFAULT_ARGS,
                tags={"gpu-arbitrage", "capstone"},
                max_active_runs=1,
        ) as dag:
            t_ingest = PythonOperator(
                task_id="ingest",
                python_callable=cfg.ingest_fn,
            )

            t_refine = self.refine_strategy.build_operator(self.pipeline_config)

            t_ext_table = BashOperator(
                task_id="register_external_tables",
                bash_command=self.dbt.run_operation(
                    "stage_external_sources",
                    cfg.external_table_selector,
                ),
            )

            t_dbt_run = BashOperator(
                task_id="process",
                bash_command=self.dbt.run(cfg.dbt_tag),
            )

            t_dbt_test = BashOperator(
                task_id="test",
                bash_command=self.dbt.test(cfg.dbt_tag),
                outlets=[Asset(f"gpu_arbitrage/{cfg.name}")],
            )

            t_ingest >> t_refine >> t_ext_table >> t_dbt_run >> t_dbt_test

        return dag
