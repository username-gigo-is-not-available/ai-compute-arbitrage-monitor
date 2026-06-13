import os

from pydantic import Field, BaseModel


class ExchangeRateConfig(BaseModel):
    enabled: bool
    base_url: str
    from_currency: str
    to_currency: str
    api_key: str = Field(default_factory=lambda: os.getenv("EXCHANGE_RATE_API_KEY", ""))

    @property
    def url(self) -> str:
        return f"{self.base_url}/{self.api_key}/latest/{self.from_currency}"
