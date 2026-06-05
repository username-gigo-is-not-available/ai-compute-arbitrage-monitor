from __future__ import annotations

from abc import ABC, abstractmethod

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.sdk import BaseOperator

from common.enums import ExecutionType
from config.loader import CloudRunConfig, ConfigLoader
from transform_adapter import DbtAdapter


class TransformStrategy(ABC):

    @abstractmethod
    def build_run_operator(self, tag: str, task_id: str = "process") -> BaseOperator:
        raise NotImplementedError

    @abstractmethod
    def build_test_operator(self, tag: str, task_id: str = "test") -> BaseOperator:
        raise NotImplementedError

    @abstractmethod
    def build_run_operation_operator(
        self, operation_name: str, tag: str, task_id: str = "register_external_tables"
    ) -> BaseOperator:
        raise NotImplementedError

    @staticmethod
    def build_strategy(config: ConfigLoader) -> "TransformStrategy":
        execution_type = config.get_execution_type()
        dbt_adapter = DbtAdapter(config.get_dbt())
        if execution_type == ExecutionType.GCP:
            return CloudRunTransformStrategy(cloud_run_config=config.get_cloud_run(), dbt=dbt_adapter)
        elif execution_type == ExecutionType.LOCAL:
            return LocalTransformStrategy(dbt=dbt_adapter)
        else:
            raise NotImplementedError(f"No dbt strategy for execution_type={execution_type}")


class LocalTransformStrategy(TransformStrategy):

    def __init__(self, dbt_adapter: DbtAdapter) -> None:
        self.dbt_adapter = dbt_adapter

    def build_run_operator(self, tag: str, task_id: str = "process") -> BaseOperator:
        return BashOperator(task_id=task_id, bash_command=self.dbt_adapter.run(tag))

    def build_test_operator(self, tag: str, task_id: str = "test") -> BaseOperator:
        return BashOperator(task_id=task_id, bash_command=self.dbt_adapter.test(tag))

    def build_run_operation_operator(
        self, operation_name: str, tag: str, task_id: str = "register_external_tables"
    ) -> BaseOperator:
        return BashOperator(
            task_id=task_id,
            bash_command=self.dbt_adapter.run_operation(operation_name, tag),
        )


class CloudRunTransformStrategy(TransformStrategy):

    def __init__(self, cloud_run_config: CloudRunConfig, dbt_adapter: DbtAdapter) -> None:
        self.cloud_run_config = cloud_run_config
        self.dbt_adapter = dbt_adapter

    def build_operator(self, dbt_args: list[str], task_id: str) -> BaseOperator:
        return CloudRunExecuteJobOperator(
            task_id=task_id,
            project_id=self.cloud_run_config.project_id,
            region=self.cloud_run_config.region_name,
            job_name=self.cloud_run_config.job_name,
            overrides={
                "container_overrides": [
                    {
                        "args": dbt_args,
                    }
                ]
            },
            deferrable=True,
        )

    def build_run_operator(self, tag: str, task_id: str = "process") -> BaseOperator:
        return self.build_operator(["run", "--select", tag], task_id)

    def build_test_operator(self, tag: str, task_id: str = "test") -> BaseOperator:
        return self.build_operator(["test", "--select", tag], task_id)

    def build_run_operation_operator(
        self, operation_name: str, tag: str, task_id: str = "register_external_tables"
    ) -> BaseOperator:
        return self.build_operator(
            ["run-operation", operation_name, "--args", f"select: {tag}"], task_id
        )
