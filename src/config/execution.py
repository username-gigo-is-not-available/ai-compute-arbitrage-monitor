from dataclasses import dataclass


@dataclass(frozen=True)
class CloudRunConfig:
    project_id: str
    region_name: str
    job_name: str
