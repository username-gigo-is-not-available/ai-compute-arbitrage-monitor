"""Silver tables are synced by the same sync_table as Bronze, from their Spark schema (ADR-022)."""

from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import assign_fresh_schema_ids
from pyiceberg.transforms import DayTransform, HourTransform
from pyspark.sql.pandas.types import to_arrow_schema

from lakehouse.partition import Partition
from lakehouse.sync import sync_table
from refine.load_strategy import OverwriteByPartition, SilverTable
from refine.schemas.compute_offers import COMPUTE_OFFERS_SILVER_SCHEMA
from refine.schemas.electricity_tariff_tiers import ELECTRICITY_TARIFF_TIERS_SILVER_SCHEMA

COMPUTE_OFFERS = SilverTable(
    COMPUTE_OFFERS_SILVER_SCHEMA, OverwriteByPartition(Partition("snapshot_at", DayTransform()))
)
TABLE = ("silver_sources", "compute_offers")


class TestIcebergSchema:
    def test_columns_follow_the_spark_schema_then_the_meta_columns(self):
        names = [f.name for f in COMPUTE_OFFERS.iceberg_schema().fields]
        assert names == [f.name for f in COMPUTE_OFFERS_SILVER_SCHEMA.fields] + ["ingested_at", "processed_at"]

    def test_every_column_is_optional(self):
        assert not any(f.required for f in COMPUTE_OFFERS.iceberg_schema().fields)

    def test_types_carry_over(self):
        schema = COMPUTE_OFFERS.iceberg_schema()
        assert str(schema.find_field("snapshot_at").field_type) == "timestamptz"
        assert str(schema.find_field("offer_id").field_type) == "int"
        assert str(schema.find_field("gpu_ids").field_type) == "list<long>"


class TestPartitionSpec:
    def test_day_transform_partitions_by_day(self):
        spec = COMPUTE_OFFERS.partition_spec()
        assert [(f.name, str(f.transform)) for f in spec.fields] == [("snapshot_at_day", "day")]

    def test_default_transform_partitions_by_identity(self):
        spec = SilverTable(ELECTRICITY_TARIFF_TIERS_SILVER_SCHEMA, OverwriteByPartition()).partition_spec()
        assert [(f.name, str(f.transform)) for f in spec.fields] == [("valid_from", "identity")]


def test_existing_spark_created_table_converges(catalog):
    # As Spark created it: required columns from nullable=False, partitioned by hour(snapshot_at)
    spark_created = assign_fresh_schema_ids(
        Catalog._convert_schema_if_needed(to_arrow_schema(COMPUTE_OFFERS.table_schema()))
    )
    source_id = spark_created.find_field("snapshot_at").field_id
    hourly = PartitionSpec(
        PartitionField(source_id=source_id, field_id=1000, transform=HourTransform(), name="snapshot_at_hour")
    )
    catalog.create_table_if_not_exists(TABLE, schema=spark_created, partition_spec=hourly)

    sync_table(catalog, TABLE, schema=COMPUTE_OFFERS.iceberg_schema(), partition_spec=COMPUTE_OFFERS.partition_spec())

    table = catalog.load_table(TABLE)
    assert not any(f.required for f in table.schema().fields)
    assert [(f.name, str(f.transform)) for f in table.spec().fields] == [("snapshot_at_day", "day")]
