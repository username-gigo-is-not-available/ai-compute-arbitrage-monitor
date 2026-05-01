from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.sdk import Asset
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator, DataprocCreateBatchOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from src.config.cluster import GCPClusterConfig
from src.config.storage import GCPStorageConfig
from infra.airflow.dags.pipeline_config import PipelineConfig

DEFAULT_ARGS = {
    "owner": "gigo",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


class DagFactory:

    def __init__(self, pipeline_config: PipelineConfig,
                 cluster_config: GCPClusterConfig,
                 storage_config: GCPStorageConfig,
                 dbt_project_dir: str,
                 dbt_target_dir: str
                 ) -> None:
        self.pipeline_config = pipeline_config
        self.cluster_config = cluster_config
        self.storage_config = storage_config
        self.dbt_directory = dbt_project_dir
        self.dbt_target_dir = dbt_target_dir

    @property
    def batch_id(self) -> str:
        return f"{self.pipeline_config.name.replace("_", "-")}-{{{{ ts_nodash | lower }}}}"

    def dataproc_batch(self) -> dict:
        image_tag = self.cluster_config.image_tag
        settings_uri = f"gs://{self.storage_config.bucket_name}/config/settings.yaml"

        return {
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{self.storage_config.bucket_name}/src/refine/pipelines/{self.pipeline_config.name}.py",
                "args": [],
            },
            "runtime_config": {
                "container_image": image_tag,
                "properties": {
                    "spark.sql.parquet.compression.codec": "snappy",
                    "spark.executorEnv.SETTINGS_PATH": settings_uri,
                    "spark.yarn.appMasterEnv.SETTINGS_PATH": settings_uri,
                },
            },
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri": self.cluster_config.subnetwork_name,
                    "service_account": self.cluster_config.service_account_email,
                },
            },
        }

    def dbt_run_operation(self, name: str, tag: str) -> str:
        return (
            f"/opt/airflow/.venv/bin/dbt run-operation {name}"
            f" --project-dir {self.dbt_directory}"
            f" --profiles-dir {self.dbt_directory}"
            f" --target-path {self.dbt_target_dir}"
            f" --args 'select: {tag}'"
        )

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
                python_callable=cfg.run_fn,
            )

            t_refine = DataprocCreateBatchOperator(
                task_id="refine",
                project_id=self.cluster_config.project_id,
                region=self.cluster_config.region_name,
                batch=self.dataproc_batch(),
                batch_id=self.batch_id,
            )

            t_ext_table = BashOperator(
                task_id="register_external_tables",
                bash_command=self.dbt_run_operation(
                    "stage_external_sources",
                    cfg.external_table_selector,
                ),
            )

            t_dbt_run = BashOperator(
                task_id="process",
                bash_command=self.dbt_run(cfg.dbt_tag),
            )

            t_dbt_test = BashOperator(
                task_id="test",
                bash_command=self.dbt_test(cfg.dbt_tag),
                outlets=[Asset(f"gpu_arbitrage/{cfg.name}")],
            )

            t_ingest >> t_refine >> t_ext_table >> t_dbt_run >> t_dbt_test

        return dag
