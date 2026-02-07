"""Tests for Iceberg ETL operations and pipeline."""

import sys
import os

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.exceptions import IcebergError


# ============== connect_catalog ==============


class TestConnectCatalog:
    @patch("etls.iceberg_etl.load_catalog")
    def test_connect_catalog_glue(self, mock_load):
        from etls.iceberg_etl import connect_catalog

        mock_catalog = MagicMock()
        mock_load.return_value = mock_catalog

        result = connect_catalog("glue", "glue", region_name="us-east-1")

        mock_load.assert_called_once_with("glue", type="glue", region_name="us-east-1")
        assert result == mock_catalog

    @patch("etls.iceberg_etl.load_catalog")
    def test_connect_catalog_sqlite(self, mock_load):
        from etls.iceberg_etl import connect_catalog

        mock_catalog = MagicMock()
        mock_load.return_value = mock_catalog

        result = connect_catalog("local", "sqlite", warehouse="/tmp/warehouse")

        mock_load.assert_called_once_with("local", type="sqlite", warehouse="/tmp/warehouse")
        assert result == mock_catalog

    @patch("etls.iceberg_etl.load_catalog")
    def test_connect_catalog_failure(self, mock_load):
        from etls.iceberg_etl import connect_catalog

        mock_load.side_effect = Exception("Connection refused")

        with pytest.raises(IcebergError, match="Failed to connect"):
            connect_catalog("glue", "glue")


# ============== create_namespace ==============


class TestCreateNamespace:
    def test_create_namespace_new(self, mock_iceberg_catalog):
        from etls.iceberg_etl import create_namespace

        create_namespace(mock_iceberg_catalog, "datalake")

        mock_iceberg_catalog.create_namespace.assert_called_once_with("datalake")

    def test_create_namespace_already_exists(self, mock_iceberg_catalog):
        from pyiceberg.exceptions import NamespaceAlreadyExistsError
        from etls.iceberg_etl import create_namespace

        mock_iceberg_catalog.create_namespace.side_effect = NamespaceAlreadyExistsError("datalake")

        # Should not raise
        create_namespace(mock_iceberg_catalog, "datalake")

    def test_create_namespace_failure(self, mock_iceberg_catalog):
        from etls.iceberg_etl import create_namespace

        mock_iceberg_catalog.create_namespace.side_effect = RuntimeError("Permission denied")

        with pytest.raises(IcebergError, match="Failed to create namespace"):
            create_namespace(mock_iceberg_catalog, "datalake")


# ============== _build_schema_from_dataframe ==============


class TestBuildSchema:
    def test_build_schema_basic(self):
        from etls.iceberg_etl import _build_schema_from_dataframe

        df = pd.DataFrame({"id": ["1", "2"], "count": [10, 20], "score": [1.5, 2.5]})
        schema = _build_schema_from_dataframe(df)

        field_names = [f.name for f in schema.fields]
        assert "id" in field_names
        assert "count" in field_names
        assert "score" in field_names

    def test_build_schema_handles_unknown_dtype(self):
        from etls.iceberg_etl import _build_schema_from_dataframe
        from pyiceberg.types import StringType

        df = pd.DataFrame({"data": [{"nested": "value"}]})
        schema = _build_schema_from_dataframe(df)

        # Unknown dtypes should default to StringType
        assert isinstance(schema.fields[0].field_type, StringType)


# ============== create_table ==============


class TestCreateTable:
    def test_create_table_new(self, mock_iceberg_catalog):
        from pyiceberg.exceptions import NoSuchTableError
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType
        from etls.iceberg_etl import create_table

        mock_table = MagicMock()
        mock_iceberg_catalog.load_table.side_effect = NoSuchTableError("not found")
        mock_iceberg_catalog.create_table.return_value = mock_table

        schema = Schema(NestedField(field_id=1, name="id", field_type=StringType(), required=False))

        result = create_table(mock_iceberg_catalog, "datalake", "test_table", schema)

        mock_iceberg_catalog.create_table.assert_called_once()
        assert result == mock_table

    def test_create_table_already_exists(self, mock_iceberg_catalog):
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType
        from etls.iceberg_etl import create_table

        mock_table = MagicMock()
        mock_iceberg_catalog.load_table.return_value = mock_table

        schema = Schema(NestedField(field_id=1, name="id", field_type=StringType(), required=False))

        result = create_table(mock_iceberg_catalog, "datalake", "test_table", schema)

        # Should load existing table, not create
        mock_iceberg_catalog.create_table.assert_not_called()
        assert result == mock_table

    def test_create_table_failure(self, mock_iceberg_catalog):
        from pyiceberg.exceptions import NoSuchTableError
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType
        from etls.iceberg_etl import create_table

        mock_iceberg_catalog.load_table.side_effect = NoSuchTableError("not found")
        mock_iceberg_catalog.create_table.side_effect = RuntimeError("S3 access denied")

        schema = Schema(NestedField(field_id=1, name="id", field_type=StringType(), required=False))

        with pytest.raises(IcebergError, match="Failed to create table"):
            create_table(mock_iceberg_catalog, "datalake", "test_table", schema)


# ============== append_data ==============


class TestAppendData:
    def test_append_data_success(self, mock_iceberg_table):
        from etls.iceberg_etl import append_data

        df = pd.DataFrame({"id": ["1", "2"], "message": ["hello", "world"]})

        result = append_data(mock_iceberg_table, df)

        mock_iceberg_table.append.assert_called_once()
        assert result["records_added"] == 2
        assert result["snapshot_id"] == "1234567890"

    def test_append_data_empty_dataframe(self, mock_iceberg_table):
        from etls.iceberg_etl import append_data

        df = pd.DataFrame({"id": [], "message": []})

        result = append_data(mock_iceberg_table, df)

        assert result["records_added"] == 0

    def test_append_data_failure(self, mock_iceberg_table):
        from etls.iceberg_etl import append_data

        mock_iceberg_table.append.side_effect = RuntimeError("Write failed")

        df = pd.DataFrame({"id": ["1"]})

        with pytest.raises(IcebergError, match="Failed to append data"):
            append_data(mock_iceberg_table, df)


# ============== get_table_metadata ==============


class TestGetTableMetadata:
    def test_get_metadata(self, mock_iceberg_table):
        from etls.iceberg_etl import get_table_metadata

        result = get_table_metadata(mock_iceberg_table)

        assert result["current_snapshot_id"] == "1234567890"
        assert result["total_snapshots"] == 1
        assert result["location"] == "s3://bucket/silver/datalake/elasticsearch_logs"

    def test_get_metadata_no_snapshot(self):
        from etls.iceberg_etl import get_table_metadata

        table = MagicMock()
        table.current_snapshot.return_value = None
        table.metadata.snapshots = []
        table.metadata.location = "s3://bucket/silver/test"

        result = get_table_metadata(table)

        assert result["current_snapshot_id"] is None
        assert result["total_snapshots"] == 0


# ============== get_snapshot_history ==============


class TestGetSnapshotHistory:
    def test_get_history(self, mock_iceberg_table):
        from etls.iceberg_etl import get_snapshot_history

        result = get_snapshot_history(mock_iceberg_table)

        assert len(result) == 1
        assert result[0]["snapshot_id"] == "1234567890"
        assert result[0]["timestamp_ms"] == 1706400000000

    def test_get_history_empty(self):
        from etls.iceberg_etl import get_snapshot_history

        table = MagicMock()
        table.metadata.snapshots = []

        result = get_snapshot_history(table)

        assert result == []


# ============== evolve_schema ==============


class TestEvolveSchema:
    def test_evolve_schema_add_columns(self, mock_iceberg_table):
        from etls.iceberg_etl import evolve_schema

        mock_update = MagicMock()
        mock_update.add_column.return_value = mock_update
        mock_iceberg_table.update_schema.return_value = mock_update

        additions = [
            {"name": "new_field", "type": "string"},
            {"name": "count", "type": "long"},
        ]

        evolve_schema(mock_iceberg_table, additions)

        assert mock_update.add_column.call_count == 2
        mock_update.commit.assert_called_once()

    def test_evolve_schema_failure(self, mock_iceberg_table):
        from etls.iceberg_etl import evolve_schema

        mock_iceberg_table.update_schema.side_effect = RuntimeError("Schema conflict")

        with pytest.raises(IcebergError, match="Failed to evolve schema"):
            evolve_schema(mock_iceberg_table, [{"name": "x", "type": "string"}])


# ============== iceberg_pipeline (integration) ==============


class TestIcebergPipeline:
    @patch("pipelines.iceberg_pipeline.connect_catalog")
    @patch("pipelines.iceberg_pipeline.create_namespace")
    @patch("pipelines.iceberg_pipeline.create_table")
    @patch("pipelines.iceberg_pipeline.append_data")
    def test_pipeline_success(self, mock_append, mock_create_table, mock_create_ns, mock_connect):
        from pipelines.iceberg_pipeline import iceberg_pipeline

        mock_catalog = MagicMock()
        mock_connect.return_value = mock_catalog

        mock_table = MagicMock()
        mock_table.metadata.location = "s3://bucket/silver/datalake/elasticsearch_logs"
        mock_create_table.return_value = mock_table

        mock_append.return_value = {"snapshot_id": "9999", "records_added": 3}

        df = pd.DataFrame({"id": ["1", "2", "3"], "message": ["a", "b", "c"]})

        result = iceberg_pipeline(df=df, table_name="test_table", namespace="test_ns", catalog_type="sqlite")

        assert result["table_id"] == "test_ns.test_table"
        assert result["snapshot_id"] == "9999"
        assert result["records_added"] == 3
        mock_connect.assert_called_once()
        mock_create_ns.assert_called_once_with(mock_catalog, "test_ns")

    @patch("pipelines.iceberg_pipeline.connect_catalog")
    def test_pipeline_catalog_failure(self, mock_connect):
        from pipelines.iceberg_pipeline import iceberg_pipeline

        mock_connect.side_effect = IcebergError("Cannot connect")

        df = pd.DataFrame({"id": ["1"]})

        with pytest.raises(IcebergError):
            iceberg_pipeline(df=df, catalog_type="glue")


# ============== scan_table ==============


class TestScanTable:
    def test_scan_table_basic(self, mock_iceberg_table):
        from etls.iceberg_etl import scan_table

        mock_scan = MagicMock()
        mock_df = pd.DataFrame({"id": ["1", "2"], "message": ["hello", "world"]})
        mock_scan.to_pandas.return_value = mock_df
        mock_iceberg_table.scan.return_value = mock_scan

        result = scan_table(mock_iceberg_table, limit=10)

        mock_iceberg_table.scan.assert_called_once_with(limit=10)
        assert len(result) == 2
        assert list(result.columns) == ["id", "message"]

    def test_scan_table_with_snapshot_id(self, mock_iceberg_table):
        from etls.iceberg_etl import scan_table

        mock_scan = MagicMock()
        mock_scan.to_pandas.return_value = pd.DataFrame({"id": ["1"]})
        mock_iceberg_table.scan.return_value = mock_scan

        scan_table(mock_iceberg_table, limit=5, snapshot_id=1234567890)

        mock_iceberg_table.scan.assert_called_once_with(limit=5, snapshot_id=1234567890)


    def test_scan_table_failure(self, mock_iceberg_table):
        from etls.iceberg_etl import scan_table

        mock_iceberg_table.scan.side_effect = RuntimeError("Read failed")

        with pytest.raises(IcebergError, match="Failed to scan table"):
            scan_table(mock_iceberg_table)
