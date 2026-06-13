from pydantic import BaseModel


class EVNConfig(BaseModel):
    enabled: bool
    url: str
    expected_schedule_text: str
    schedule_text_selector: str = "div > div > div:nth-child(4) > p.table-paragraph"
    distribution_fee_row_selector: str = "table tr"
    distribution_fee_cell_selector: str = "td"
