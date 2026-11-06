import pytest
from bs4 import BeautifulSoup

from ingest.evn_base import EVNBaseIngestor

PAGE = BeautifulSoup("<div><strong>Блок 1</strong> до 200 kWh</div>", "html.parser")


def test_select_required_returns_the_element() -> None:
    assert EVNBaseIngestor.select_required(PAGE, "strong").text == "Блок 1"


def test_select_required_names_the_missing_selector() -> None:
    with pytest.raises(ValueError, match="'table.tariffs'"):
        EVNBaseIngestor.select_required(PAGE, "table.tariffs")


def test_next_sibling_required_returns_the_text_after() -> None:
    strong = EVNBaseIngestor.select_required(PAGE, "strong")
    assert EVNBaseIngestor.next_sibling_required(strong).text == " до 200 kWh"


def test_next_sibling_required_fails_at_the_end() -> None:
    last = BeautifulSoup("<div><strong>Блок 1</strong></div>", "html.parser").strong
    with pytest.raises(ValueError, match="Блок 1"):
        EVNBaseIngestor.next_sibling_required(last)
