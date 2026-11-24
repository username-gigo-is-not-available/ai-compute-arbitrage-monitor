import os

from pydantic import Field

from config.apis.api_key import required_api_key
from config.base import SettingsBlock


class ExchangeRateConfig(SettingsBlock):
    enabled: bool
    base_url: str
    from_currency: str
    to_currency: str
    api_key: str = Field(default_factory=lambda: os.getenv("EXCHANGE_RATE_API_KEY", ""))

    @property
    def url(self) -> str:
        return f"{self.base_url}/latest/{self.from_currency}"

    @property
    def header(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {required_api_key(self.api_key, 'EXCHANGE_RATE_API_KEY')}"}
