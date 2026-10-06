import json
import os

from pydantic import BaseModel, Field

from common.classes import AskContractIdRange
from common.enums import OfferType


class VastAIConfig(BaseModel):
    enabled: bool
    base_url: str
    # /bundles returns at most `limit` offers, a random sample when more match; a range returning fewer than
    # `split_at` is complete, a fuller one is split into `census_parts` ranges (ADR-020).
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

    def params(self, id_range: AskContractIdRange) -> dict[str, str]:
        query = {"type": OfferType.ON_DEMAND.api_value, "limit": self.limit, "ask_contract_id": id_range.query()}
        return {"q": json.dumps(query)}
