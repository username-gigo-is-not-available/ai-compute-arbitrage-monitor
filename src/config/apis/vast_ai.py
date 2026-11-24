import json
import os

from pydantic import Field

from common.enums import OfferType
from config.apis.api_key import required_api_key
from config.base import SettingsBlock


class VastAIConfig(SettingsBlock):
    enabled: bool
    base_url: str
    limit: int
    split_at: int
    census_parts: int
    request_interval_seconds: float
    api_key: str = Field(default_factory=lambda: os.getenv("VASTAI_API_KEY", ""))

    @property
    def url(self) -> str:
        return f"{self.base_url}/bundles"

    @property
    def header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {required_api_key(self.api_key, 'VASTAI_API_KEY')}"}

    def params(self, start: int, end: int | None) -> dict[str, str]:
        ask_contract_id = {"gte": start} if end is None else {"gte": start, "lt": end}
        query = {"type": OfferType.ON_DEMAND.api_value, "limit": self.limit, "ask_contract_id": ask_contract_id}
        return {"q": json.dumps(query)}
