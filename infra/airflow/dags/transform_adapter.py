import os
import shutil


class DbtAdapter:
    @property
    def dbt_bin_directory_path(self) -> str:
        return shutil.which("dbt") or "/home/airflow/.local/bin/dbt"

    @property
    def dbt_project_directory_path(self) -> str:
        return os.environ["DBT_PROJECT_DIR"]

    @property
    def dbt_target_directory_path(self) -> str:
        return os.environ["DBT_TARGET_DIR"]

    def base_flags(self) -> str:
        return (
            f" --project-dir {self.dbt_project_directory_path}"
            f" --profiles-dir {self.dbt_project_directory_path}"
            f" --target-path {self.dbt_target_directory_path}"
        )

    def run(self, tag: str) -> str:
        return f"{self.dbt_bin_directory_path} run{self.base_flags()} --select {tag}"

    def test(self, tag: str) -> str:
        return f"{self.dbt_bin_directory_path} test{self.base_flags()} --select {tag}"

    def run_operation(self, operation_name: str, tag: str) -> str:
        return f"{self.dbt_bin_directory_path} run-operation {operation_name}{self.base_flags()} --args 'select: {tag}'"
