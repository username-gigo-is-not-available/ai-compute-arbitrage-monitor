from pydantic import BaseModel


class CloudRunConfig(BaseModel):
    project_id: str
    region_name: str
    job_name: str
