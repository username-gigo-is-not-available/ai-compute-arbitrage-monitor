from google.auth import default as gcp_default
from google.auth.transport.requests import Request as GcpRequest
from openai import BaseModel


class GCPAuthentication(BaseModel):
    cloud_platform_url: str = "https://www.googleapis.com/auth/cloud-platform"

    @property
    def access_token(self) -> str:
        creds, _ = gcp_default(scopes=[self.cloud_platform_url])
        creds.refresh(GcpRequest())
        return creds.token
