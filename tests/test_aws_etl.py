"""Tests for aws_etl module."""

import os
import sys

import pytest
from unittest.mock import MagicMock, patch
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from etls.aws_etl import connect_to_s3, create_bucket_if_not_exists, upload_file_to_s3, list_s3_objects
from utils.exceptions import S3UploadError


class TestConnectToS3:
    """Tests for connect_to_s3 function."""

    @patch("etls.aws_etl.boto3.client")
    def test_connect_with_credentials(self, mock_boto_client):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        result = connect_to_s3("access_key", "secret_key", "us-east-1")

        assert result == mock_client
        mock_boto_client.assert_called_once_with(
            "s3", aws_access_key_id="access_key", aws_secret_access_key="secret_key", region_name="us-east-1"
        )

    @patch("etls.aws_etl.boto3.client")
    def test_connect_without_credentials_uses_default_chain(self, mock_boto_client):
        mock_client = MagicMock()
        mock_boto_client.return_value = mock_client

        result = connect_to_s3(None, None, "us-west-2")

        assert result == mock_client
        mock_boto_client.assert_called_once_with("s3", region_name="us-west-2")

    @patch("etls.aws_etl.boto3.client")
    def test_connect_raises_on_error(self, mock_boto_client):
        mock_boto_client.side_effect = Exception("Invalid credentials")

        with pytest.raises(Exception, match="Invalid credentials"):
            connect_to_s3("bad_key", "bad_secret", "us-east-1")


class TestCreateBucketIfNotExists:
    """Tests for create_bucket_if_not_exists function."""

    def test_bucket_already_exists(self, mock_s3_client):
        mock_s3_client.head_bucket.return_value = {}

        result = create_bucket_if_not_exists(mock_s3_client, "existing-bucket", "us-east-1")

        assert result is True
        mock_s3_client.create_bucket.assert_not_called()

    def test_create_bucket_us_east_1(self, mock_s3_client):
        """us-east-1 doesn't require LocationConstraint."""
        error_response = {"Error": {"Code": "404"}}
        mock_s3_client.head_bucket.side_effect = ClientError(error_response, "HeadBucket")
        mock_s3_client.create_bucket.return_value = {}

        result = create_bucket_if_not_exists(mock_s3_client, "new-bucket", "us-east-1")

        assert result is True
        mock_s3_client.create_bucket.assert_called_once_with(Bucket="new-bucket")

    def test_create_bucket_other_region(self, mock_s3_client):
        """Non us-east-1 regions require LocationConstraint."""
        error_response = {"Error": {"Code": "404"}}
        mock_s3_client.head_bucket.side_effect = ClientError(error_response, "HeadBucket")
        mock_s3_client.create_bucket.return_value = {}

        result = create_bucket_if_not_exists(mock_s3_client, "new-bucket", "eu-west-1")

        assert result is True
        mock_s3_client.create_bucket.assert_called_once_with(
            Bucket="new-bucket", CreateBucketConfiguration={"LocationConstraint": "eu-west-1"}
        )

    def test_create_bucket_permission_denied(self, mock_s3_client):
        error_response = {"Error": {"Code": "403"}}
        mock_s3_client.head_bucket.side_effect = ClientError(error_response, "HeadBucket")

        with pytest.raises(ClientError):
            create_bucket_if_not_exists(mock_s3_client, "forbidden-bucket", "us-east-1")


class TestUploadFileToS3:
    """Tests for upload_file_to_s3 function."""

    def test_upload_with_auto_generated_key(self, mock_s3_client, tmp_path):
        test_file = tmp_path / "test.csv"
        test_file.write_text("col1,col2\na,b")

        result = upload_file_to_s3(mock_s3_client, str(test_file), "my-bucket", prefix="bronze/data")

        assert result.startswith("s3://my-bucket/bronze/data/")
        assert "test.csv" in result
        mock_s3_client.upload_file.assert_called_once()

    def test_upload_with_custom_key(self, mock_s3_client, tmp_path):
        test_file = tmp_path / "test.csv"
        test_file.write_text("col1,col2\na,b")

        result = upload_file_to_s3(mock_s3_client, str(test_file), "my-bucket", s3_key="custom/path/file.csv")

        assert result == "s3://my-bucket/custom/path/file.csv"

    def test_upload_raises_on_client_error(self, mock_s3_client, tmp_path):
        test_file = tmp_path / "test.csv"
        test_file.write_text("data")

        error_response = {"Error": {"Code": "AccessDenied", "Message": "Access Denied"}}
        mock_s3_client.upload_file.side_effect = ClientError(error_response, "PutObject")

        with pytest.raises(S3UploadError, match="Failed to upload"):
            upload_file_to_s3(mock_s3_client, str(test_file), "my-bucket")


class TestListS3Objects:
    """Tests for list_s3_objects function."""

    def test_list_objects_with_pagination(self, mock_s3_client):
        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [
            {"Contents": [{"Key": "file1.csv"}, {"Key": "file2.csv"}]},
            {"Contents": [{"Key": "file3.csv"}]},
        ]

        result = list_s3_objects(mock_s3_client, "my-bucket", "prefix/")

        assert len(result) == 3
        assert "file1.csv" in result
        assert "file3.csv" in result
        mock_s3_client.get_paginator.assert_called_once_with("list_objects_v2")

    def test_list_empty_bucket(self, mock_s3_client):
        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{}]

        result = list_s3_objects(mock_s3_client, "empty-bucket")

        assert result == []

    def test_list_objects_with_prefix_filter(self, mock_s3_client):
        mock_paginator = MagicMock()
        mock_s3_client.get_paginator.return_value = mock_paginator
        mock_paginator.paginate.return_value = [{"Contents": [{"Key": "bronze/es/2025-01-28/data.parquet"}]}]

        result = list_s3_objects(mock_s3_client, "my-bucket", "bronze/es/")

        assert len(result) == 1
        mock_paginator.paginate.assert_called_once_with(Bucket="my-bucket", Prefix="bronze/es/")
