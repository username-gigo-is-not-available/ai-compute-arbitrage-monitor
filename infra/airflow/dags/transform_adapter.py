import os
import shutil


class DbtAdapter:
    DBT_BIN_DIR = shutil.which("dbt") or "/home/airflow/.local/bin/dbt"
    DBT_PROJECT_DIR: str = os.environ["DBT_PROJECT_DIR"]
    DBT_TARGET_DIR: str = os.environ["DBT_TARGET_DIR"]

    def base_flags(self) -> str:
        return (
            f" --project-dir {self.DBT_PROJECT_DIR}"
            f" --profiles-dir {self.DBT_PROJECT_DIR}"
            f" --target-path {self.DBT_TARGET_DIR}"
        )

    def run(self, tag: str) -> str:
        return f"{self.DBT_BIN_DIR} run{self.base_flags()} --select {tag}"

    def test(self, tag: str) -> str:
        return f"{self.DBT_BIN_DIR} test{self.base_flags()} --select {tag}"

    def run_operation(self, operation_name: str, tag: str) -> str:
        return f"{self.DBT_BIN_DIR} run-operation {operation_name}{self.base_flags()} --args 'select: {tag}'"
