from pydantic import BaseModel


class EVNConfig(BaseModel):
    enabled: bool

    # Tariff Tiers & Fees
    tariff_tiers_url: str
    household_tiers_table_selector: str = '.table-grid'
    businesses_tiers_table_selector: str = '.table-grid_mali'
    tariff_fees_table_selector: str = '.table-grid-tarifi'
    table_cells_selector: str = 'h5'
    tariff_tiers_valid_from_text_selector: str = 'div > div > div:nth-child(10) > p.table-paragraph'
    household_block_label_col: int = 2
    household_block_price_col: int = 3
    business_block_label_col: int = 1
    business_block_price_col: int = 2
    fees_label_col: int = 0
    business_fees_label_col: int = 5
    household_fees_label_col: int = 6

    # Tariff Blocks
    tariff_system_url: str
    tariff_blocks_paragraph_selector: str = 'div > div > p:nth-child(5)'
    tariff_blocks_strong_selector: str = 'strong'
    tariff_blocks_limit: int = 4
    tariff_blocks_valid_from_selector: str = 'div > div > p:nth-child(1)'

    # Tariff window schedule
    schedule_text_selector: str = "div > div > div:nth-child(4) > p.table-paragraph"
