import json
import os

from pydantic import Field

from config.file_config import FileConfig


class VastAIConfig(FileConfig):
    enabled: bool
    base_url: str
    poll_interval_seconds: int
    api_key: str = Field(default_factory=lambda: os.getenv("VASTAI_API_KEY", ""))
    limit: int
    topic_name: str

    @property
    def url(self) -> str:
        return f"{self.base_url}/bundles"

    @property
    def header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.api_key}"}

    @property
    def params(self) -> dict[str, str]:
        return {"q": json.dumps({"limit": self.limit})}
