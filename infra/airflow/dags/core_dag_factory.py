from __future__ import annotations

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import Asset

from ingest.registry import run_ingest
from transform_strategy import TransformStrategy
from pipeline_config import DAG_TAGS, DEFAULT_ARGS, PipelineConfig
from spark_job_strategy import SparkJobStrategy


class DagFactory:

    def __init__(self, pipeline_config: PipelineConfig,
                 spark_job_strategy: SparkJobStrategy,
                 transform_strategy: TransformStrategy
                 ) -> None:
        self.pipeline_config = pipeline_config
        self.spark_job_strategy = spark_job_strategy
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
                tags=DAG_TAGS,
                max_active_runs=1,
        ) as dag:
            # DDL first: the tables match their declarations before anything loads (ADR-022, #41)
            t_setup = self.spark_job_strategy.build_operator(config, package="lakehouse", task_id="setup")

            t_ingest = PythonOperator(
                task_id="ingest",
                python_callable=run_ingest,
                op_kwargs={"dataset": config.dataset_name, "scheduled_at": "{{ data_interval_end }}"},
            )

            t_refine = self.spark_job_strategy.build_operator(config, package="refine", task_id="refine")

            t_dbt_run = self.transform_strategy.build_run_operator(tag=config.dbt_tag)

            t_dbt_test = self.transform_strategy.build_test_operator(tag=config.dbt_tag)
            t_dbt_test.outlets = [Asset(config.asset_name)]

            t_setup >> t_ingest >> t_refine >> t_dbt_run >> t_dbt_test

        return dag
