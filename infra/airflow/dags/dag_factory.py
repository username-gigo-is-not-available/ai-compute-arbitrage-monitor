from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.sdk import Asset
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

from src.config.cluster import GCPClusterConfig
from src.config.storage import GCPStorageConfig
from infra.airflow.dags.data_source_config import PipelineConfig

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
    def dataproc_job(self) -> dict:
        return {
            "reference": {"project_id": self.cluster_config.project_id},
            "placement": {"cluster_name": self.cluster_config.cluster_name},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{self.storage_config.bucket_name}/{self.cluster_config.entrypoints_path}/{self.pipeline_config.name}.py",
                "python_file_uris": [f"gs://{self.storage_config.bucket_name}/src.zip"],
                "file_uris": [f"gs://{self.storage_config.bucket_name}/config/settings.yaml"],
                "properties": {
                    "spark.sql.parquet.compression.codec": "snappy",
                    "spark.yarn.appMasterEnv.SETTINGS_PATH": f"gs://{self.storage_config.bucket_name}/config/settings.yaml",
                    "spark.executorEnv.SETTINGS_PATH": f"gs://{self.storage_config.bucket_name}/config/settings.yaml",
                    "spark.pyspark.python": "/opt/gpu_arbitrage/.venv/bin/python",
                    "spark.pyspark.driver.python": "/opt/gpu_arbitrage/.venv/bin/python",
                    "spark.submit.pyFiles": f"gs://{self.storage_config.bucket_name}/src.zip",
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

            t_zip_src = BashOperator(
                task_id="zip_src",
                bash_command="rm -f /opt/airflow/src.zip && cd /opt/airflow/src && zip -r /opt/airflow/src.zip .",
            )

            t_upload_config = LocalFilesystemToGCSOperator(
                task_id="upload_config",
                src="/opt/airflow/config/settings.yaml",
                dst="config/settings.yaml",
                bucket=self.storage_config.bucket_name,
            )

            t_upload_entrypoint = LocalFilesystemToGCSOperator(
                task_id="upload_entrypoints",
                src=f"/opt/airflow/src/refine/pipelines/{self.pipeline_config.name}.py",
                dst=f"src/refine/pipelines/{self.pipeline_config.name}.py",
                bucket=self.storage_config.bucket_name,
            )

            t_upload_src = LocalFilesystemToGCSOperator(
                task_id="upload_src",
                src="/opt/airflow/src.zip",
                dst="src.zip",
                bucket=self.storage_config.bucket_name,
            )

            t_refine = DataprocSubmitJobOperator(
                task_id="refine",
                job=self.dataproc_job,
                region=self.cluster_config.region_name,
                project_id=self.cluster_config.project_id,
            )

            t_ext_table = BashOperator(
                task_id="register_external_tables",
                bash_command=self.dbt_run_operation("stage_external_sources",
                                                    self.pipeline_config.external_table_selector),
            )

            t_dbt_run = BashOperator(
                task_id="process",
                bash_command=self.dbt_run(self.pipeline_config.dbt_tag),
            )

            t_dbt_test = BashOperator(
                task_id="test",
                bash_command=self.dbt_test(self.pipeline_config.dbt_tag),
                outlets=[Asset(f"gpu_arbitrage/{cfg.name}")],
            )

            t_ingest >> \
            t_zip_src >> \
            [
                t_upload_src,
                t_upload_config,
                t_upload_entrypoint
            ] >> \
            t_refine >> \
            t_ext_table >> \
            t_dbt_run >> \
            t_dbt_test

        return dag
