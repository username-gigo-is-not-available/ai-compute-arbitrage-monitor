from abc import ABC
from dataclasses import dataclass
from http import HTTPStatus

import certifi
import requests
from bs4 import BeautifulSoup

from config.http import HttpConfig
from ingest.base import SyncIngestor


@dataclass
class EVNBaseIngestor(SyncIngestor, ABC):
    http_config: HttpConfig

    def fetch_soup(self, url: str) -> BeautifulSoup | None:
        timeout_seconds: int = self.http_config.timeout_seconds
        response = requests.get(url, timeout=timeout_seconds, verify=certifi.where())
        status: int = response.status_code
        if status != HTTPStatus.OK:
            self.logger.error(f"EVN tariff fetch returned HTTP status: {status}.")
            return None
        return BeautifulSoup(response.text, "html.parser")

