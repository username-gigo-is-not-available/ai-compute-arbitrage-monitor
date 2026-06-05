from dataclasses import dataclass


@dataclass(frozen=True)
class DbtConfig:
    project_directory_path: str
    profiles_directory_path: str
    target_directory_path: str