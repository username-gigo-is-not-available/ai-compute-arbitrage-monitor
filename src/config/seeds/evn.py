from config.file_config import FileConfig


class EVNConfig(FileConfig):
    enabled: bool
    url: str
    expected_schedule_text: str
    schedule_text_selector: str = "div > div > div:nth-child(4) > p.table-paragraph"
