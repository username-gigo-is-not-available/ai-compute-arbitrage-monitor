import os

from pydantic import BaseModel, Field

from config.apis.api_key import required_api_key


class ExchangeRateConfig(BaseModel):
    enabled: bool
    base_url: str
    from_currency: str
    to_currency: str
    api_key: str = Field(default_factory=lambda: os.getenv("EXCHANGE_RATE_API_KEY", ""))

    @property
    def url(self) -> str:
        api_key = required_api_key(self.api_key, "EXCHANGE_RATE_API_KEY")
        return f"{self.base_url}/{api_key}/latest/{self.from_currency}"
