"""Iceberg table operations for the lakehouse Silver layer."""

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    BooleanType,
    DoubleType,
    FloatType,
    LongType,
    NestedField,
    StringType,
    TimestampType,
    TimestamptzType,
)

from utils.exceptions import IcebergError

logger = logging.getLogger(__name__)

# Mapping from pandas/pyarrow dtypes to Iceberg types
_DTYPE_MAP = {
    "object": StringType(),
    "string": StringType(),
    "int64": LongType(),
    "float64": DoubleType(),
    "float32": FloatType(),
    "bool": BooleanType(),
    "datetime64[ns]": TimestampType(),
    "datetime64[ns, UTC]": TimestamptzType(),
}


def connect_catalog(catalog_name: str, catalog_type: str, **properties) -> object:
    """
    Connect to an Iceberg catalog.

    Args:
        catalog_name: Name identifier for the catalog
        catalog_type: Catalog backend type ("glue" or "sqlite")
        **properties: Additional catalog properties (region_name, warehouse, uri, etc.)

    Returns:
        Iceberg Catalog instance

    Raises:
        IcebergError: If catalog connection fails
    """
    try:
        catalog_properties = {"type": catalog_type, **properties}
        catalog = load_catalog(catalog_name, **catalog_properties)
        logger.info(f"Connected to Iceberg catalog '{catalog_name}' (type={catalog_type})")
        return catalog
    except Exception as e:
        raise IcebergError(f"Failed to connect to Iceberg catalog '{catalog_name}': {e}") from e


def create_namespace(catalog, namespace: str) -> None:
    """
    Create a namespace (database) in the catalog if it doesn't exist.

    Args:
        catalog: Iceberg Catalog instance
        namespace: Namespace name (e.g., "datalake")

    Raises:
        IcebergError: If namespace creation fails
    """
    try:
        catalog.create_namespace(namespace)
        logger.info(f"Created namespace '{namespace}'")
    except NamespaceAlreadyExistsError:
        logger.info(f"Namespace '{namespace}' already exists")
    except Exception as e:
        raise IcebergError(f"Failed to create namespace '{namespace}': {e}") from e


def _build_schema_from_dataframe(df) -> Schema:
    """
    Build an Iceberg Schema from a Pandas DataFrame.

    Args:
        df: Pandas DataFrame

    Returns:
        Iceberg Schema
    """
    fields = []
    for i, (col_name, dtype) in enumerate(df.dtypes.items(), start=1):
        dtype_str = str(dtype)
        if dtype_str in _DTYPE_MAP:
            iceberg_type = _DTYPE_MAP[dtype_str]
        elif hasattr(dtype, "tz") and dtype.tz is not None:
            iceberg_type = TimestamptzType()
        elif dtype_str.startswith("datetime64"):
            iceberg_type = TimestampType()
        else:
            iceberg_type = StringType()
        fields.append(NestedField(field_id=i, name=col_name, field_type=iceberg_type, required=False))
    return Schema(*fields)


def create_table(
    catalog, namespace: str, table_name: str, schema: Schema, partition_spec: PartitionSpec = None
) -> Table:
    """
    Create an Iceberg table if it doesn't exist.

    Args:
        catalog: Iceberg Catalog instance
        namespace: Namespace name
        table_name: Table name
        schema: Iceberg Schema
        partition_spec: Partition specification (default: partition by day on extracted_at)

    Returns:
        Iceberg Table instance

    Raises:
        IcebergError: If table creation fails
    """
    table_identifier = f"{namespace}.{table_name}"

    try:
        table = catalog.load_table(table_identifier)
        logger.info(f"Table '{table_identifier}' already exists")
        return table
    except NoSuchTableError:
        pass

    try:
        if partition_spec is None:
            # Find the extracted_at field for default partitioning
            extracted_at_field = None
            for field in schema.fields:
                if field.name == "extracted_at":
                    extracted_at_field = field
                    break

            if extracted_at_field:
                partition_spec = PartitionSpec(
                    PartitionField(
                        source_id=extracted_at_field.field_id,
                        field_id=1000,
                        transform=DayTransform(),
                        name="extracted_at_day",
                    )
                )
            else:
                partition_spec = PartitionSpec()

        table = catalog.create_table(
            identifier=table_identifier,
            schema=schema,
            partition_spec=partition_spec,
        )
        logger.info(f"Created Iceberg table '{table_identifier}'")
        return table
    except TableAlreadyExistsError:
        return catalog.load_table(table_identifier)
    except Exception as e:
        raise IcebergError(f"Failed to create table '{table_identifier}': {e}") from e


def append_data(table: Table, df) -> dict:
    """
    Append a Pandas DataFrame to an Iceberg table.

    Args:
        table: Iceberg Table instance
        df: Pandas DataFrame to append

    Returns:
        dict with snapshot_id and records_added

    Raises:
        IcebergError: If data append fails
    """
    try:
        arrow_table = pa.Table.from_pandas(df, preserve_index=False)
        # Downcast timestamp[ns] to timestamp[us] for Iceberg compatibility,
        # preserving timezone info (timestamptz vs timestamp).
        # Also cast any list/struct columns to string as a fallback,
        # in case nested data was not flattened upstream.
        new_fields = []
        for field in arrow_table.schema:
            if pa.types.is_timestamp(field.type) and field.type.unit == "ns":
                new_fields.append(field.with_type(pa.timestamp("us", tz=field.type.tz)))
            elif pa.types.is_list(field.type) or pa.types.is_struct(field.type):
                new_fields.append(field.with_type(pa.string()))
            else:
                new_fields.append(field)
        arrow_table = arrow_table.cast(pa.schema(new_fields))
        table.append(arrow_table)

        current_snapshot = table.current_snapshot()
        snapshot_id = str(current_snapshot.snapshot_id) if current_snapshot else None

        result = {
            "snapshot_id": snapshot_id,
            "records_added": len(df),
        }
        logger.info(f"Appended {len(df)} records to '{table.name()}', snapshot: {snapshot_id}")
        return result
    except Exception as e:
        raise IcebergError(f"Failed to append data to table '{table.name()}': {e}") from e


def get_table_metadata(table: Table) -> dict:
    """
    Get metadata about an Iceberg table.

    Args:
        table: Iceberg Table instance

    Returns:
        dict with schema, partitioning, snapshot info, and location
    """
    current_snapshot = table.current_snapshot()
    snapshots = table.metadata.snapshots

    return {
        "table_name": table.name(),
        "schema": str(table.schema()),
        "partition_spec": str(table.spec()),
        "location": table.metadata.location,
        "current_snapshot_id": str(current_snapshot.snapshot_id) if current_snapshot else None,
        "total_snapshots": len(snapshots),
    }


def get_snapshot_history(table: Table) -> list:
    """
    Get the snapshot history of an Iceberg table.

    Args:
        table: Iceberg Table instance

    Returns:
        List of dicts with snapshot_id, timestamp, and summary
    """
    snapshots = table.metadata.snapshots
    history = []

    for snapshot in snapshots:
        summary = {}
        if snapshot.summary:
            try:
                if hasattr(snapshot.summary, "operation"):
                    summary["operation"] = str(snapshot.summary.operation)
                # Access the private _additional_properties dict directly
                # to avoid Summary.__getitem__ which calls .lower() on keys
                props = getattr(snapshot.summary, "_additional_properties", None)
                if props and isinstance(props, dict):
                    for k, v in props.items():
                        summary[str(k)] = str(v)
            except Exception:
                summary = {"raw": str(snapshot.summary)}

        history.append(
            {
                "snapshot_id": str(snapshot.snapshot_id),
                "timestamp_ms": snapshot.timestamp_ms,
                "summary": summary,
            }
        )

    return history


def scan_table(table: Table, limit: int = 100, snapshot_id: int | None = None) -> pd.DataFrame:
    """
    Scan an Iceberg table and return rows as a Pandas DataFrame.

    Args:
        table: Iceberg Table instance
        limit: Maximum number of rows to return (default 100)
        snapshot_id: Optional snapshot ID for time-travel queries

    Returns:
        Pandas DataFrame with scanned rows

    Raises:
        IcebergError: If scan operation fails
    """
    try:
        scan_kwargs = {"limit": limit}
        if snapshot_id is not None:
            scan_kwargs["snapshot_id"] = snapshot_id

        scan = table.scan(**scan_kwargs)
        df = scan.to_pandas()

        logger.info(f"Scanned '{table.name()}': {len(df)} rows" + (f" (snapshot={snapshot_id})" if snapshot_id else ""))
        return df
    except Exception as e:
        raise IcebergError(f"Failed to scan table '{table.name()}': {e}") from e


def expire_snapshots(table: Table, older_than_days: int = 7) -> int:
    """
    Expire snapshots older than a specified number of days.

    Args:
        table: Iceberg Table instance
        older_than_days: Remove snapshots older than this many days

    Returns:
        Number of snapshots expired

    Raises:
        IcebergError: If snapshot expiration fails
    """
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(days=older_than_days)
        cutoff_ms = int(cutoff.timestamp() * 1000)

        # Filter snapshots to expire
        to_expire = [s for s in table.metadata.snapshots if s.timestamp_ms < cutoff_ms]

        if not to_expire:
            logger.info("No snapshots to expire")
            return 0

        manager = table.manage_snapshots()
        for snapshot in to_expire:
            manager = manager.remove(snapshot.snapshot_id)
        manager.commit()

        expired_count = len(to_expire)
        logger.info(f"Expired {expired_count} snapshots older than {older_than_days} days")
        return expired_count
    except Exception as e:
        raise IcebergError(f"Failed to expire snapshots: {e}") from e


def evolve_schema(table: Table, additions: list) -> None:
    """
    Add new columns to an Iceberg table schema.

    Args:
        table: Iceberg Table instance
        additions: List of dicts with 'name' and 'type' keys.
                   Type values: "string", "long", "double", "boolean", "timestamp", "timestamptz"

    Raises:
        IcebergError: If schema evolution fails
    """
    type_map = {
        "string": StringType(),
        "long": LongType(),
        "double": DoubleType(),
        "float": FloatType(),
        "boolean": BooleanType(),
        "timestamp": TimestampType(),
        "timestamptz": TimestamptzType(),
    }

    try:
        update = table.update_schema()
        for col in additions:
            iceberg_type = type_map.get(col["type"], StringType())
            update = update.add_column(col["name"], iceberg_type)
        update.commit()
        logger.info(f"Schema evolved: added {len(additions)} column(s)")
    except Exception as e:
        raise IcebergError(f"Failed to evolve schema: {e}") from e
