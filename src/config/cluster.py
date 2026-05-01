from pydantic import BaseModel


class GCPClusterConfig(BaseModel):
    project_id: str
    region_name: str
    image_tag: str
    runtime_packages: list[str]
    entrypoints_path: str
    subnetwork_name: str
    batch_id_prefix: str
    service_account_email: str
