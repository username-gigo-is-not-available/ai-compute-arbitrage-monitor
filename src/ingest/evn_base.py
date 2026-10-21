from abc import ABC
from dataclasses import dataclass

import certifi
import requests
from bs4 import BeautifulSoup

from ingest.base import SyncIngestor


@dataclass
class EVNBaseIngestor(SyncIngestor, ABC):

    def fetch_soup(self, url: str) -> BeautifulSoup:
        timeout_seconds: int = self.http_config.timeout_seconds

        response = self.fetch_sync(
            exception_type=requests.RequestException,
            http_get_function=requests.get,
            url=url,
            timeout=timeout_seconds,
            verify=certifi.where(),
        )
        response.raise_for_status()
        return BeautifulSoup(response.text, "html.parser")

