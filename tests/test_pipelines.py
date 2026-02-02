"""Integration tests for pipeline modules."""

import os
import sys

import pytest
import pandas as pd
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.elasticsearch_pipeline import elasticsearch_pipeline
from pipelines.aws_s3_pipeline import upload_s3_pipeline
from utils.exceptions import ElasticsearchConnectionError


class TestElasticsearchPipeline:
    """Integration tests for the Elasticsearch extraction pipeline."""

    @patch("pipelines.elasticsearch_pipeline.connect_elasticsearch")
    @patch("pipelines.elasticsearch_pipeline.extract_documents")
    def test_pipeline_csv_output(self, mock_extract, mock_connect, tmp_path):
        """Test full pipeline produces a valid CSV file."""
        mock_connect.return_value = MagicMock()
        mock_extract.return_value = [
            {"id": "1", "message": "test1", "level": "INFO"},
            {"id": "2", "message": "test2", "level": "ERROR"},
        ]

        with patch("pipelines.elasticsearch_pipeline.OUTPUT_PATH", str(tmp_path)):
            result = elasticsearch_pipeline(
                file_name="test_output", use_time_filter=False, output_format="csv", validate=True
            )

        assert result.endswith(".csv")
        assert os.path.exists(result)

        df = pd.read_csv(result)
        assert len(df) == 2
        assert "extracted_at" in df.columns

    @patch("pipelines.elasticsearch_pipeline.connect_elasticsearch")
    @patch("pipelines.elasticsearch_pipeline.extract_documents")
    def test_pipeline_parquet_output(self, mock_extract, mock_connect, tmp_path):
        """Test full pipeline produces a valid Parquet file."""
        mock_connect.return_value = MagicMock()
        mock_extract.return_value = [
            {"id": "1", "message": "test1"},
            {"id": "2", "message": "test2"},
        ]

        with patch("pipelines.elasticsearch_pipeline.OUTPUT_PATH", str(tmp_path)):
            result = elasticsearch_pipeline(
                file_name="test_output", use_time_filter=False, output_format="parquet", validate=True
            )

        assert result.endswith(".parquet")
        assert os.path.exists(result)

        df = pd.read_parquet(result)
        assert len(df) == 2

    @patch("pipelines.elasticsearch_pipeline.connect_elasticsearch")
    @patch("pipelines.elasticsearch_pipeline.extract_documents")
    def test_pipeline_raises_on_empty_extraction(self, mock_extract, mock_connect):
        """Test pipeline raises ValueError when no documents are extracted."""
        mock_connect.return_value = MagicMock()
        mock_extract.return_value = []

        with pytest.raises(ValueError, match="No documents extracted"):
            elasticsearch_pipeline(
                file_name="test_output",
                use_time_filter=False,
            )

    @patch("pipelines.elasticsearch_pipeline.connect_elasticsearch")
    def test_pipeline_raises_on_connection_failure(self, mock_connect):
        """Test pipeline propagates connection errors."""
        mock_connect.side_effect = ElasticsearchConnectionError("Connection refused")

        with pytest.raises(ElasticsearchConnectionError):
            elasticsearch_pipeline(
                file_name="test_output",
                use_time_filter=False,
            )

    @patch("pipelines.elasticsearch_pipeline.connect_elasticsearch")
    @patch("pipelines.elasticsearch_pipeline.extract_documents_by_time_range")
    def test_pipeline_uses_time_filter(self, mock_extract_time, mock_connect, tmp_path):
        """Test pipeline calls time-range extraction when use_time_filter=True."""
        mock_connect.return_value = MagicMock()
        mock_extract_time.return_value = [
            {"id": "1", "message": "test1"},
        ]

        with patch("pipelines.elasticsearch_pipeline.OUTPUT_PATH", str(tmp_path)):
            elasticsearch_pipeline(
                file_name="test_output", use_time_filter=True, time_range_hours=12, output_format="csv", validate=False
            )

        mock_extract_time.assert_called_once()
        call_kwargs = mock_extract_time.call_args[1]
        assert call_kwargs["index"] is not None


class TestS3UploadPipeline:
    """Integration tests for the S3 upload pipeline."""

    @patch("pipelines.aws_s3_pipeline.upload_file_to_s3")
    @patch("pipelines.aws_s3_pipeline.create_bucket_if_not_exists")
    @patch("pipelines.aws_s3_pipeline.connect_to_s3")
    def test_upload_specific_file(self, mock_connect, mock_bucket, mock_upload, tmp_path):
        """Test uploading a specific file to S3."""
        mock_connect.return_value = MagicMock()
        mock_bucket.return_value = True
        mock_upload.return_value = "s3://bucket/raw/elasticsearch/2025-01-28/test.csv"

        test_file = tmp_path / "test.csv"
        test_file.write_text("id,message\n1,test")

        result = upload_s3_pipeline(file_path=str(test_file))

        assert result.startswith("s3://")
        mock_upload.assert_called_once()

    @patch("pipelines.aws_s3_pipeline.connect_to_s3")
    @patch("pipelines.aws_s3_pipeline.create_bucket_if_not_exists")
    def test_upload_raises_on_missing_file(self, mock_bucket, mock_connect):
        """Test pipeline raises FileNotFoundError for non-existent file."""
        mock_connect.return_value = MagicMock()
        mock_bucket.return_value = True

        with pytest.raises(FileNotFoundError, match="File not found"):
            upload_s3_pipeline(file_path="/nonexistent/path/file.csv")

    @patch("pipelines.aws_s3_pipeline.OUTPUT_PATH", "/empty/dir/that/does/not/exist")
    @patch("pipelines.aws_s3_pipeline.connect_to_s3")
    @patch("pipelines.aws_s3_pipeline.create_bucket_if_not_exists")
    def test_upload_raises_when_no_files_in_output(self, mock_bucket, mock_connect):
        """Test pipeline raises FileNotFoundError when output dir has no files."""
        mock_connect.return_value = MagicMock()
        mock_bucket.return_value = True

        with pytest.raises(FileNotFoundError, match="No output files found"):
            upload_s3_pipeline(file_path=None)

    @patch("pipelines.aws_s3_pipeline.upload_file_to_s3")
    @patch("pipelines.aws_s3_pipeline.create_bucket_if_not_exists")
    @patch("pipelines.aws_s3_pipeline.connect_to_s3")
    def test_upload_finds_latest_file(self, mock_connect, mock_bucket, mock_upload, tmp_path):
        """Test pipeline picks the latest file when no file_path is given."""
        mock_connect.return_value = MagicMock()
        mock_bucket.return_value = True
        mock_upload.return_value = "s3://bucket/raw/data.parquet"

        # Create two files
        (tmp_path / "old.csv").write_text("old data")
        (tmp_path / "new.parquet").write_text("new data")

        with patch("pipelines.aws_s3_pipeline.OUTPUT_PATH", str(tmp_path)):
            upload_s3_pipeline(file_path=None)

        mock_upload.assert_called_once()
