from __future__ import annotations

from abc import ABC, abstractmethod

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator

from common.enums import ExecutionType
from config.cluster import GCPClusterConfig
from config.loader import ConfigLoader
from config.storage import GCPStorageConfig
from infra.airflow.dags.pipeline_config import PipelineConfig


class RefineStrategy(ABC):

    @abstractmethod
    def build_operator(self, pipeline_config: PipelineConfig) -> BaseOperator:
        raise NotImplementedError

    @staticmethod
    def build_strategy(config: ConfigLoader) -> RefineStrategy:
        execution_type: ExecutionType = config.get_execution_type()
        if execution_type == ExecutionType.GCP:
            return DataprocRefineStrategy(
                cluster_config=config.get_cluster(),
                storage_config=config.get_storage()
            )
        elif execution_type == ExecutionType.LOCAL:
            return LocalRefineStrategy()
        else:
            raise NotImplementedError

class LocalRefineStrategy(RefineStrategy):

    def build_operator(self, pipeline_config: PipelineConfig) -> BaseOperator:
        return PythonOperator(
            task_id="refine",
            python_callable=pipeline_config.refine_fn,
        )


class DataprocRefineStrategy(RefineStrategy):

    def __init__(self, cluster_config: GCPClusterConfig, storage_config: GCPStorageConfig) -> None:
        self.cluster_config = cluster_config
        self.storage_config = storage_config

    def batch_config(self, pipeline_config: PipelineConfig) -> dict:
        entrypoint_uri = (
            f"gs://{self.storage_config.bucket_name}"
            f"/{self.cluster_config.entrypoints_path}"
            f"/{pipeline_config.name}.py"
        )
        return {
            "pyspark_batch": {
                "main_python_file_uri": entrypoint_uri,
                "args": [],
            },
            "runtime_config": {
                "container_image": self.cluster_config.image_tag,
                "properties": {
                    "spark.sql.parquet.compression.codec": "snappy",
                },
            },
            "environment_config": {
                "execution_config": {
                    "subnetwork_uri": self.cluster_config.subnetwork_name,
                    "service_account": self.cluster_config.service_account_email,
                },
            },
        }

    @staticmethod
    def generate_batch_id(pipeline_config: PipelineConfig) -> str:
        return f"{pipeline_config.name.replace('_', '-')}-{{{{ ts_nodash | lower }}}}"

    def build_operator(self, pipeline_config: PipelineConfig) -> BaseOperator:
        return DataprocCreateBatchOperator(
            task_id="refine",
            project_id=self.cluster_config.project_id,
            region=self.cluster_config.region_name,
            batch=self.batch_config(pipeline_config),
            batch_id=self.generate_batch_id(pipeline_config),
        )
