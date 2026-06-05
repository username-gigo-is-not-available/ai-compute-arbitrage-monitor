import os
import shutil

from config.dbt import DbtConfig


class DbtAdapter:
    def __init__(self, dbt_config: DbtConfig) -> None:
        self.dbt_config = dbt_config

    @property
    def dbt_bin_directory_path(self) -> str:
        return shutil.which("dbt") or "/home/airflow/.local/bin/dbt"

    def base_flags(self) -> str:
        return (
            f" --project-dir {self.dbt_config.project_directory_path}"
            f" --profiles-dir {self.dbt_config.profiles_directory_path}"
            f" --target-path {self.dbt_config.target_directory_path}"
        )

    def run(self, tag: str) -> str:
        return f"{self.dbt_bin_directory_path} run{self.base_flags()} --select {tag}"

    def test(self, tag: str) -> str:
        return f"{self.dbt_bin_directory_path} test{self.base_flags()} --select {tag}"

    def run_operation(self, operation_name: str, tag: str) -> str:
        return f"{self.dbt_bin_directory_path} run-operation {operation_name}{self.base_flags()} --args 'select: {tag}'"
