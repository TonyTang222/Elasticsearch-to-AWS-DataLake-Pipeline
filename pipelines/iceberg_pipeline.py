"""Iceberg pipeline for writing transformed data to the Silver layer."""

import logging

import pandas as pd

from etls.iceberg_etl import (
    _build_schema_from_dataframe,
    append_data,
    connect_catalog,
    create_namespace,
    create_table,
)
from utils.constants import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    ICEBERG_CATALOG_NAME,
    ICEBERG_CATALOG_TYPE,
    ICEBERG_NAMESPACE,
    ICEBERG_TABLE_NAME,
    ICEBERG_WAREHOUSE_PATH,
)

logger = logging.getLogger(__name__)


def iceberg_pipeline(
    df: pd.DataFrame,
    table_name: str = None,
    namespace: str = None,
    catalog_type: str = None,
) -> dict:
    """
    Write a DataFrame to an Iceberg table in the Silver layer.

    Args:
        df: Pandas DataFrame to write
        table_name: Iceberg table name (default from config)
        namespace: Iceberg namespace (default from config)
        catalog_type: Catalog type - "glue" or "sqlite" (default from config)

    Returns:
        dict with table_id, snapshot_id, records_added, location

    Raises:
        IcebergError: If any Iceberg operation fails
    """
    table_name = table_name or ICEBERG_TABLE_NAME
    namespace = namespace or ICEBERG_NAMESPACE
    catalog_type = catalog_type or ICEBERG_CATALOG_TYPE

    logger.info(f"Starting Iceberg pipeline: {namespace}.{table_name}")

    # Build catalog connection properties
    # Use client.* prefix for unified credentials (covers both Glue and S3)
    catalog_properties = {"warehouse": ICEBERG_WAREHOUSE_PATH}
    if AWS_REGION:
        catalog_properties["client.region"] = AWS_REGION
    if AWS_ACCESS_KEY_ID:
        catalog_properties["client.access-key-id"] = AWS_ACCESS_KEY_ID
    if AWS_SECRET_ACCESS_KEY:
        catalog_properties["client.secret-access-key"] = AWS_SECRET_ACCESS_KEY

    # Connect to catalog
    catalog = connect_catalog(
        catalog_name=ICEBERG_CATALOG_NAME,
        catalog_type=catalog_type,
        **catalog_properties,
    )

    # Ensure namespace exists
    create_namespace(catalog, namespace)

    # Build schema from DataFrame and create table
    schema = _build_schema_from_dataframe(df)
    table = create_table(catalog, namespace, table_name, schema)

    # Append data
    result = append_data(table, df)

    table_id = f"{namespace}.{table_name}"
    output = {
        "table_id": table_id,
        "snapshot_id": result["snapshot_id"],
        "records_added": result["records_added"],
        "location": table.metadata.location,
    }
    logger.info(f"Iceberg pipeline completed: {output}")
    return output
