from google.auth import default as gcp_default
from google.auth.transport.requests import Request as GcpRequest

CLOUD_PLATFORM_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def access_token() -> str:
    """A fresh ADC bearer token; pyiceberg's REST client and Spark's local catalog both take one explicitly."""
    creds, _ = gcp_default(scopes=[CLOUD_PLATFORM_SCOPE])
    creds.refresh(GcpRequest())
    return creds.token
