"""Shared pytest fixtures for the test suite."""

import sys
import os

import pytest
import pandas as pd
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


@pytest.fixture
def sample_es_response():
    """Sample Elasticsearch search response with 2 documents."""
    return {
        "_scroll_id": "test_scroll_id",
        "hits": {
            "total": {"value": 2, "relation": "eq"},
            "hits": [
                {
                    "_id": "doc1",
                    "_index": "test-index",
                    "_score": 1.0,
                    "_source": {
                        "@timestamp": "2025-01-28T10:00:00Z",
                        "message": "Test message 1",
                        "level": "INFO",
                        "service": "api-gateway",
                    },
                },
                {
                    "_id": "doc2",
                    "_index": "test-index",
                    "_score": 0.9,
                    "_source": {
                        "@timestamp": "2025-01-28T11:00:00Z",
                        "message": "Test message 2",
                        "level": "ERROR",
                        "service": "auth-service",
                    },
                },
            ],
        },
    }


@pytest.fixture
def empty_es_response():
    """Empty Elasticsearch search response."""
    return {"_scroll_id": "test_scroll_id", "hits": {"total": {"value": 0, "relation": "eq"}, "hits": []}}


@pytest.fixture
def sample_dataframe():
    """Sample DataFrame for testing transformations and validations."""
    return pd.DataFrame(
        {
            "id": ["1", "2", "3"],
            "@timestamp": ["2025-01-28T10:00:00Z", "2025-01-28T11:00:00Z", "2025-01-28T12:00:00Z"],
            "message": ["msg1", "msg2", "msg3"],
            "level": ["INFO", "ERROR", "WARN"],
        }
    )


@pytest.fixture
def mock_elasticsearch_client():
    """Mock Elasticsearch client with ping returning True."""
    mock_client = MagicMock()
    mock_client.ping.return_value = True
    return mock_client


@pytest.fixture
def mock_s3_client():
    """Mock boto3 S3 client."""
    return MagicMock()


@pytest.fixture
def test_client():
    """FastAPI test client with fresh run store."""
    from fastapi.testclient import TestClient
    from api.main import app
    from api.run_store import RunStore
    from concurrent.futures import ThreadPoolExecutor

    app.state.run_store = RunStore()
    app.state.executor = ThreadPoolExecutor(max_workers=1)
    with TestClient(app) as client:
        yield client
    app.state.executor.shutdown(wait=False)


@pytest.fixture
def run_store():
    """Isolated RunStore instance for unit tests."""
    from api.run_store import RunStore

    return RunStore()


@pytest.fixture
def mock_iceberg_catalog():
    """Mock Iceberg catalog for testing."""
    catalog = MagicMock()
    catalog.list_namespaces.return_value = []
    return catalog


@pytest.fixture
def mock_iceberg_table():
    """Mock Iceberg table with snapshot support."""
    table = MagicMock()
    table.name.return_value = "datalake.elasticsearch_logs"

    # Mock current snapshot
    mock_snapshot = MagicMock()
    mock_snapshot.snapshot_id = 1234567890
    mock_snapshot.timestamp_ms = 1706400000000
    mock_snapshot.summary = {"added-data-files": "1", "added-records": "100"}
    table.current_snapshot.return_value = mock_snapshot

    # Mock metadata
    table.metadata.snapshots = [mock_snapshot]
    table.metadata.location = "s3://bucket/silver/datalake/elasticsearch_logs"
    table.schema.return_value = MagicMock()
    table.spec.return_value = MagicMock()

    return table
