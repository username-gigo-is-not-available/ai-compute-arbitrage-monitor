import json
import os

from pydantic import BaseModel, Field


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

    def params(self, lo: int, hi: int | None) -> dict[str, str]:
        ask_contract_id = {"gte": lo} if hi is None else {"gte": lo, "lt": hi}
        return {"q": json.dumps({"type": "on-demand", "limit": self.limit, "ask_contract_id": ask_contract_id})}
