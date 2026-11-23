from pyiceberg.catalog.rest import RestCatalog

from config.auth import GCPAuthentication
from config.lakehouse import GCPLakehouseConfig


# Here, not on the config: Airflow imports config/ to build the DAGs and has no pyiceberg (ADR-028, #116)
def open_catalog(lakehouse: GCPLakehouseConfig) -> RestCatalog:
    return RestCatalog(
        lakehouse.catalog_id,
        **{
            **lakehouse.catalog_props(),
            "token": GCPAuthentication().access_token,
            "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        },
    )
