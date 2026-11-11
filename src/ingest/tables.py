from common.enums import DatasetName
from ingest.schemas.compute_offers import COMPUTE_OFFERS_BRONZE_SCHEMA
from ingest.schemas.electricity_tariff_blocks import ELECTRICITY_TARIFF_BLOCKS_BRONZE_SCHEMA
from ingest.schemas.electricity_tariff_fees import ELECTRICITY_TARIFF_FEES_BRONZE_SCHEMA
from ingest.schemas.electricity_tariff_tiers import ELECTRICITY_TARIFF_TIERS_BRONZE_SCHEMA
from ingest.schemas.electricity_tariff_window_schedule import ELECTRICITY_TARIFF_WINDOW_SCHEDULE_BRONZE_SCHEMA
from ingest.schemas.exchange_rates import EXCHANGE_RATES_BRONZE_SCHEMA
from ingest.write_strategy import AppendByDay, BronzeTable

# Imports only schemas and pyiceberg, so lakehouse.setup can read it without the ingestors' HTTP libraries
BRONZE_TABLES: dict[DatasetName, BronzeTable] = {
    DatasetName.COMPUTE_OFFERS: BronzeTable(COMPUTE_OFFERS_BRONZE_SCHEMA, AppendByDay(column="snapshot_at")),
    DatasetName.EXCHANGE_RATES: BronzeTable(EXCHANGE_RATES_BRONZE_SCHEMA, AppendByDay(column="timestamp")),
    DatasetName.ELECTRICITY_TARIFF_TIERS: BronzeTable(
        ELECTRICITY_TARIFF_TIERS_BRONZE_SCHEMA, AppendByDay(column="ingested_at")
    ),
    DatasetName.ELECTRICITY_TARIFF_FEES: BronzeTable(
        ELECTRICITY_TARIFF_FEES_BRONZE_SCHEMA, AppendByDay(column="ingested_at")
    ),
    DatasetName.ELECTRICITY_TARIFF_BLOCKS: BronzeTable(
        ELECTRICITY_TARIFF_BLOCKS_BRONZE_SCHEMA, AppendByDay(column="ingested_at")
    ),
    DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE: BronzeTable(
        ELECTRICITY_TARIFF_WINDOW_SCHEDULE_BRONZE_SCHEMA, AppendByDay(column="ingested_at")
    ),
}
