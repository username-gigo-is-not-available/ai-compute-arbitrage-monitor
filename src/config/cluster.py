from pydantic import BaseModel


class GCPClusterConfig(BaseModel):
    project_id: str
    region_name: str
    cluster_name: str
    entrypoints_path: str