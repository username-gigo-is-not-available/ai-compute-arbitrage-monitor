from __future__ import annotations

from abc import ABC, abstractmethod

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import BaseOperator

from common.enums import ExecutionType
from config.bucket import GCSBucketConfig
from config.cluster import GCPClusterConfig
from config.loader import ConfigLoader

from pipeline_config import PipelineConfig


# Runs a package's main.py with the dataset as its one argument, where pyspark is installed: lakehouse (the setup
# task) and refine. The DAG starts it as a command and never imports the package (Composer has no pyspark).
class SparkJobStrategy(ABC):

    @abstractmethod
    def build_operator(self, pipeline_config: PipelineConfig, package: str, task_id: str) -> BaseOperator:
        raise NotImplementedError

    @staticmethod
    def build_strategy(config: ConfigLoader) -> SparkJobStrategy:
        execution_type: ExecutionType = config.get_execution_type()
        if execution_type == ExecutionType.GCP:
            return DataprocSparkJobStrategy(
                cluster_config=config.get_cluster(),
                bucket_config=config.get_bucket()
            )
        elif execution_type == ExecutionType.LOCAL:
            return LocalSparkJobStrategy()
        else:
            raise NotImplementedError


class LocalSparkJobStrategy(SparkJobStrategy):

    # The same entry script Dataproc runs, in its own process
    @staticmethod
    def command(pipeline_config: PipelineConfig, package: str) -> str:
        return f"python -m {package}.main {pipeline_config.dataset_name}"

    def build_operator(self, pipeline_config: PipelineConfig, package: str, task_id: str) -> BaseOperator:
        return BashOperator(task_id=task_id, bash_command=self.command(pipeline_config, package))


class DataprocSparkJobStrategy(SparkJobStrategy):

    def __init__(self, cluster_config: GCPClusterConfig, bucket_config: GCSBucketConfig) -> None:
        self.cluster_config = cluster_config
        self.bucket_config = bucket_config

    def batch_config(self, pipeline_config: PipelineConfig, package: str) -> dict:
        return {
            "pyspark_batch": {
                "main_python_file_uri": f"gs://{self.bucket_config.bucket_name}/jobs/{package}/main.py",
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

    # The task id is part of it: setup and refine run in the same DAG run, and batch ids must be unique
    @staticmethod
    def generate_batch_id(pipeline_config: PipelineConfig, task_id: str) -> str:
        return f"{task_id}-{pipeline_config.dataset_name.replace('_', '-')}-{{{{ ts_nodash | lower }}}}"

    def build_operator(self, pipeline_config: PipelineConfig, package: str, task_id: str) -> BaseOperator:
        return DataprocCreateBatchOperator(
            task_id=task_id,
            project_id=self.cluster_config.project_id,
            region=self.cluster_config.region_name,
            batch=self.batch_config(pipeline_config, package),
            batch_id=self.generate_batch_id(pipeline_config, task_id),
            deferrable=True
        )
