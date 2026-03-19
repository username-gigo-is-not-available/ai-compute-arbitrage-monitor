from config.file_config import FileConfig


class ERCConfig(FileConfig):
    enabled: bool
    url: str
    data: dict[str, int]
    table_rows_selector: str = "table > tr:has(td)"
    type_selector: str = "td:nth-child(1)"
    price_selector: str = "td:nth-child(2)"
    valid_from_selector: str = "td:nth-child(4)"
