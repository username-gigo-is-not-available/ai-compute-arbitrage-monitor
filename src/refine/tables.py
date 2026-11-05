from pyiceberg.transforms import DayTransform

from common.enums import DatasetName
from lakehouse.partition import Partition
from refine.load_strategy import OverwriteByPartition, SilverTable
from refine.schemas.compute_offers import COMPUTE_OFFERS_SILVER_SCHEMA
from refine.schemas.electricity_tariff_blocks import ELECTRICITY_TARIFF_BLOCKS_SILVER_SCHEMA
from refine.schemas.electricity_tariff_fees import ELECTRICITY_TARIFF_FEES_SILVER_SCHEMA
from refine.schemas.electricity_tariff_tiers import ELECTRICITY_TARIFF_TIERS_SILVER_SCHEMA
from refine.schemas.electricity_tariff_window_schedule import ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SILVER_SCHEMA
from refine.schemas.exchange_rates import EXCHANGE_RATES_SILVER_SCHEMA

# What each Silver table looks like: its schema and partition (the DDL half; refine.registry is the DML half).
# Imports only schemas, pyspark types and pyiceberg, never the pipelines.
SILVER_TABLES: dict[DatasetName, SilverTable] = {
    DatasetName.COMPUTE_OFFERS:
        SilverTable(COMPUTE_OFFERS_SILVER_SCHEMA, OverwriteByPartition(Partition("snapshot_at", DayTransform()))),
    DatasetName.EXCHANGE_RATES:
        SilverTable(EXCHANGE_RATES_SILVER_SCHEMA, OverwriteByPartition(Partition("timestamp"))),
    DatasetName.ELECTRICITY_TARIFF_TIERS:
        SilverTable(ELECTRICITY_TARIFF_TIERS_SILVER_SCHEMA, OverwriteByPartition()),
    DatasetName.ELECTRICITY_TARIFF_FEES:
        SilverTable(ELECTRICITY_TARIFF_FEES_SILVER_SCHEMA, OverwriteByPartition()),
    DatasetName.ELECTRICITY_TARIFF_BLOCKS:
        SilverTable(ELECTRICITY_TARIFF_BLOCKS_SILVER_SCHEMA, OverwriteByPartition()),
    DatasetName.ELECTRICITY_TARIFF_WINDOW_SCHEDULE:
        SilverTable(ELECTRICITY_TARIFF_WINDOW_SCHEDULE_SILVER_SCHEMA, OverwriteByPartition()),
}
