import json
import os

from pydantic import BaseModel, Field

from common.enums import OfferType


class VastAIConfig(BaseModel):
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
        return {"Authorization": f"Bearer {self.api_key}"}

    def params(self, start: int, end: int | None) -> dict[str, str]:
        ask_contract_id = {"gte": start} if end is None else {"gte": start, "lt": end}
        query = {"type": OfferType.ON_DEMAND.api_value, "limit": self.limit, "ask_contract_id": ask_contract_id}
        return {"q": json.dumps(query)}
