import json
import os

from pydantic import Field

from config.file_config import FileConfig
from common.enums import OfferType


class VastAIConfig(FileConfig):
    enabled: bool
    base_url: str
    api_key: str = Field(default_factory=lambda: os.getenv("VASTAI_API_KEY", ""))
    limit: int

    @property
    def url(self) -> str:
        return f"{self.base_url}/bundles"

    @property
    def header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.api_key}"}


    def params(self, offer_type: OfferType) -> dict[str, str]:
        return {"q": json.dumps({"limit": self.limit, "type": offer_type.value})}

