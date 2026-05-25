from pydantic import BaseModel


class GCPClusterConfig(BaseModel):
    project_id: str
    region_name: str
    image_tag: str
    runtime_packages: list[str]
    subnetwork_name: str
    service_account_email: str
