from abc import ABC
from dataclasses import dataclass
from typing import Generic

from bs4 import BeautifulSoup, PageElement, Tag

from config.apis.evn import EVNConfig
from ingest.base import Ingestor, RecordT


@dataclass
class EVNBaseIngestor(Ingestor[EVNConfig, RecordT], ABC, Generic[RecordT]):
    def fetch_soup(self, url: str) -> BeautifulSoup:
        return BeautifulSoup(self.http.get(url).text, "html.parser")

    # The EVN pages have no API: a missing element means the page changed, so fail with what was looked for
    @staticmethod
    def select_required(parent: Tag, selector: str) -> Tag:
        element = parent.select_one(selector)
        if element is None:
            raise ValueError(f"EVN page has no element matching {selector!r}; did the page layout change?")
        return element

    @staticmethod
    def next_sibling_required(element: Tag) -> PageElement:
        if element.next_sibling is None:
            raise ValueError(f"EVN page has nothing after {element.text!r}; did the page layout change?")
        return element.next_sibling
