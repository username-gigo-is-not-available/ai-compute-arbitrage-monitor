from __future__ import annotations

from abc import ABC, abstractmethod

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import BaseOperator

from common.enums import ExecutionType
from config.cluster import GCPClusterConfig
from config.loader import ConfigLoader
from config.bucket import GCSBucketConfig
from pipeline_config import PipelineConfig


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
                bucket_config=config.get_bucket()
            )
        elif execution_type == ExecutionType.LOCAL:
            return LocalRefineStrategy()
        else:
            raise NotImplementedError


class LocalRefineStrategy(RefineStrategy):

    def build_operator(self, pipeline_config: PipelineConfig) -> BaseOperator:
        # Imported here, not at the top: Composer parses this file but does not ship the refine package
        # (gcp.composer.runtime_packages in settings.yaml)
        from refine.registry import run_refine

        return PythonOperator(
            task_id="refine",
            python_callable=run_refine,
            op_kwargs={"dataset": pipeline_config.dataset_name},
        )


class DataprocRefineStrategy(RefineStrategy):

    def __init__(self, cluster_config: GCPClusterConfig, bucket_config: GCSBucketConfig) -> None:
        self.cluster_config = cluster_config
        self.bucket_config = bucket_config

    def batch_config(self, pipeline_config: PipelineConfig) -> dict:
        return {
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{self.bucket_config.bucket_name}/jobs/refine/main.py",
                "args": [pipeline_config.dataset_name],
                "python_file_uris": [
                    f"gs://{self.bucket_config.bucket_name}/jobs/modules.zip",
                ],
                "file_uris": [
                    f"gs://{self.bucket_config.bucket_name}/jobs/config/settings.yaml",
                ],
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
        return f"{pipeline_config.dataset_name.replace('_', '-')}-{{{{ ts_nodash | lower }}}}"

    def build_operator(self, pipeline_config: PipelineConfig) -> BaseOperator:
        return DataprocCreateBatchOperator(
            task_id="refine",
            project_id=self.cluster_config.project_id,
            region=self.cluster_config.region_name,
            batch=self.batch_config(pipeline_config),
            batch_id=self.generate_batch_id(pipeline_config),
            deferrable=True
        )
