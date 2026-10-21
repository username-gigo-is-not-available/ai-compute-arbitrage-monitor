from abc import ABC
from dataclasses import dataclass

from bs4 import BeautifulSoup

from ingest.base import Ingestor


@dataclass
class EVNBaseIngestor(Ingestor, ABC):

    def fetch_soup(self, url: str) -> BeautifulSoup:
        return BeautifulSoup(self.fetch(url).text, "html.parser")
