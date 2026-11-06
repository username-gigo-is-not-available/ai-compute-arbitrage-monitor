"""Silver schemas must not narrow numbers to 32-bit FloatType (#33).

Bronze carries 64-bit doubles; cast_to_schema would truncate prices and rates
(53.7295 -> 53.72949981689453) on the way into Silver.
"""

import importlib
import pkgutil

from pyspark.sql.types import FloatType, StructType

import refine.schemas


def _silver_schemas() -> dict[str, StructType]:
    schemas: dict[str, StructType] = {}
    for module_info in pkgutil.iter_modules(refine.schemas.__path__):
        module = importlib.import_module(f"refine.schemas.{module_info.name}")
        for name, value in vars(module).items():
            if isinstance(value, StructType):
                schemas[f"{module_info.name}.{name}"] = value
    return schemas


def test_schemas_are_discovered():
    assert "exchange_rates.EXCHANGE_RATES_SILVER_SCHEMA" in _silver_schemas()


def test_no_float32_columns():
    narrowed = [
        f"{schema_name}.{field.name}"
        for schema_name, schema in _silver_schemas().items()
        for field in schema.fields
        if isinstance(field.dataType, FloatType)
    ]
    assert narrowed == []
