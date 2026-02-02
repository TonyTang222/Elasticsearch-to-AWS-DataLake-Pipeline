"""Tests for elasticsearch_etl module."""

import os
import sys

import pytest
import pandas as pd
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etls.elasticsearch_etl import (
    connect_elasticsearch,
    extract_documents,
    transform_data,
    load_data_to_csv,
    load_data_to_parquet,
)
from utils.exceptions import ElasticsearchConnectionError, ElasticsearchExtractionError


class TestConnectElasticsearch:
    """Tests for connect_elasticsearch function."""

    @patch("etls.elasticsearch_etl.Elasticsearch")
    def test_connect_success_without_auth(self, mock_es_class):
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_es_class.return_value = mock_client

        result = connect_elasticsearch("localhost", 9200)

        assert result == mock_client
        mock_es_class.assert_called_once()
        mock_client.ping.assert_called_once()

    @patch("etls.elasticsearch_etl.Elasticsearch")
    def test_connect_success_with_auth(self, mock_es_class):
        mock_client = MagicMock()
        mock_client.ping.return_value = True
        mock_es_class.return_value = mock_client

        result = connect_elasticsearch("localhost", 9200, "user", "pass")

        assert result == mock_client
        call_kwargs = mock_es_class.call_args
        assert call_kwargs[1]["basic_auth"] == ("user", "pass")

    @patch("etls.elasticsearch_etl.Elasticsearch")
    def test_connect_ping_fails(self, mock_es_class):
        mock_client = MagicMock()
        mock_client.ping.return_value = False
        mock_es_class.return_value = mock_client

        with pytest.raises(ElasticsearchConnectionError, match="Failed to ping"):
            connect_elasticsearch("localhost", 9200)

    @patch("etls.elasticsearch_etl.Elasticsearch")
    def test_connect_raises_on_exception(self, mock_es_class):
        mock_es_class.side_effect = Exception("Connection refused")

        with pytest.raises(ElasticsearchConnectionError, match="Connection refused"):
            connect_elasticsearch("localhost", 9200)


class TestExtractDocuments:
    """Tests for extract_documents function."""

    def test_extract_documents_success(self, mock_elasticsearch_client, sample_es_response):
        mock_elasticsearch_client.search.return_value = sample_es_response
        mock_elasticsearch_client.scroll.return_value = {"_scroll_id": "test_scroll_id", "hits": {"hits": []}}

        result = extract_documents(mock_elasticsearch_client, "test-index")

        assert len(result) == 2
        assert result[0]["id"] == "doc1"
        assert result[0]["message"] == "Test message 1"
        assert result[1]["id"] == "doc2"
        mock_elasticsearch_client.clear_scroll.assert_called_once()

    def test_extract_documents_empty_index(self, mock_elasticsearch_client, empty_es_response):
        mock_elasticsearch_client.search.return_value = empty_es_response

        result = extract_documents(mock_elasticsearch_client, "empty-index")

        assert result == []
        mock_elasticsearch_client.clear_scroll.assert_called_once()

    def test_extract_documents_custom_query(self, mock_elasticsearch_client, sample_es_response):
        mock_elasticsearch_client.search.return_value = sample_es_response
        mock_elasticsearch_client.scroll.return_value = {"_scroll_id": "test_scroll_id", "hits": {"hits": []}}

        custom_query = {"match": {"level": "ERROR"}}
        extract_documents(mock_elasticsearch_client, "test-index", query=custom_query)

        call_kwargs = mock_elasticsearch_client.search.call_args[1]
        assert call_kwargs["query"] == custom_query

    def test_extract_documents_clears_scroll_on_error(self, mock_elasticsearch_client):
        """Verify scroll context is cleaned up even when extraction fails."""
        mock_elasticsearch_client.search.return_value = {
            "_scroll_id": "test_scroll_id",
            "hits": {"hits": [{"_id": "1", "_index": "test", "_score": 1.0, "_source": {}}]},
        }
        mock_elasticsearch_client.scroll.side_effect = Exception("Scroll error")

        with pytest.raises(ElasticsearchExtractionError):
            extract_documents(mock_elasticsearch_client, "test-index")

        mock_elasticsearch_client.clear_scroll.assert_called_once_with(scroll_id="test_scroll_id")

    def test_extract_documents_default_match_all_query(self, mock_elasticsearch_client, empty_es_response):
        mock_elasticsearch_client.search.return_value = empty_es_response

        extract_documents(mock_elasticsearch_client, "test-index")

        call_kwargs = mock_elasticsearch_client.search.call_args[1]
        assert call_kwargs["query"] == {"match_all": {}}


class TestTransformData:
    """Tests for transform_data function."""

    def test_transform_data_empty_dataframe(self):
        df = pd.DataFrame()
        result = transform_data(df)
        assert result.empty

    def test_transform_data_adds_extracted_at(self):
        df = pd.DataFrame({"id": ["1", "2"], "message": ["test1", "test2"]})
        result = transform_data(df)
        assert "extracted_at" in result.columns

    def test_transform_data_converts_timestamp(self):
        df = pd.DataFrame({"id": ["1"], "@timestamp": ["2025-01-28T10:00:00Z"]})
        result = transform_data(df)
        assert pd.api.types.is_datetime64_any_dtype(result["@timestamp"])

    def test_transform_data_handles_nested_dict(self):
        df = pd.DataFrame({"id": ["1"], "nested": [{"key": "value"}]})
        result = transform_data(df)
        assert isinstance(result["nested"].iloc[0], str)

    def test_transform_data_handles_invalid_timestamp(self):
        df = pd.DataFrame({"id": ["1"], "@timestamp": ["not-a-timestamp"]})
        result = transform_data(df)
        assert pd.isna(result["@timestamp"].iloc[0])

    def test_transform_data_preserves_other_columns(self, sample_dataframe):
        result = transform_data(sample_dataframe)
        assert "message" in result.columns
        assert "level" in result.columns
        assert list(result["message"]) == ["msg1", "msg2", "msg3"]


class TestLoadDataToCsv:
    """Tests for load_data_to_csv function."""

    def test_load_data_to_csv_success(self, sample_dataframe, tmp_path):
        output_path = str(tmp_path / "test_output.csv")

        result = load_data_to_csv(sample_dataframe, output_path)

        assert result == output_path
        assert os.path.exists(output_path)

        loaded_df = pd.read_csv(output_path)
        assert len(loaded_df) == 3
        assert "id" in loaded_df.columns


class TestLoadDataToParquet:
    """Tests for load_data_to_parquet function."""

    def test_load_data_to_parquet_success(self, sample_dataframe, tmp_path):
        output_path = str(tmp_path / "test_output.parquet")

        result = load_data_to_parquet(sample_dataframe, output_path)

        assert result == output_path
        assert os.path.exists(output_path)

        loaded_df = pd.read_parquet(output_path)
        assert len(loaded_df) == 3
        assert "id" in loaded_df.columns

    def test_load_data_to_parquet_with_gzip(self, sample_dataframe, tmp_path):
        output_path = str(tmp_path / "test_output_gzip.parquet")

        result = load_data_to_parquet(sample_dataframe, output_path, compression="gzip")

        assert result == output_path
        assert os.path.exists(output_path)
