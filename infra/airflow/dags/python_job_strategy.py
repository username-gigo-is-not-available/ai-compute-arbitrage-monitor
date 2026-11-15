from __future__ import annotations

from abc import ABC, abstractmethod

from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import BaseOperator

from common.enums import ExecutionType
from config.execution import CloudRunConfig
from config.loader import ConfigLoader


# Setup and ingest: our plain-Python programs, started as `python -m <module> <args>`, never imported (ADR-028)
class PythonJobStrategy(ABC):
    @abstractmethod
    def build_operator(self, module: str, args: list[str], task_id: str) -> BaseOperator:
        raise NotImplementedError

    @staticmethod
    def build_strategy(config: ConfigLoader) -> PythonJobStrategy:
        execution_type: ExecutionType = config.get_execution_type()
        if execution_type == ExecutionType.GCP:
            return CloudRunPythonJobStrategy(cloud_run_config=config.get_cloud_run("python"))
        elif execution_type == ExecutionType.LOCAL:
            return LocalPythonJobStrategy()
        else:
            raise NotImplementedError(f"No python job strategy for execution_type={execution_type}")


class LocalPythonJobStrategy(PythonJobStrategy):
    def build_operator(self, module: str, args: list[str], task_id: str) -> BaseOperator:
        return BashOperator(task_id=task_id, bash_command=" ".join(["python", "-m", module, *args]))


class CloudRunPythonJobStrategy(PythonJobStrategy):
    def __init__(self, cloud_run_config: CloudRunConfig) -> None:
        self.cloud_run_config = cloud_run_config

    # The image's entry point is `python -m`, so the args start with the module
    def build_operator(self, module: str, args: list[str], task_id: str) -> BaseOperator:
        return CloudRunExecuteJobOperator(
            task_id=task_id,
            project_id=self.cloud_run_config.project_id,
            region=self.cloud_run_config.region_name,
            job_name=self.cloud_run_config.job_name,
            overrides={"container_overrides": [{"args": [module, *args]}]},
            deferrable=True,
        )
