import logging
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from utils.exceptions import S3UploadError

logger = logging.getLogger(__name__)


def connect_to_s3(aws_access_key_id: str = None, aws_secret_access_key: str = None, region: str = "us-east-1"):
    """
    Create S3 client connection.

    If credentials are not provided, boto3 will use the default credential chain:
    1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
    2. Shared credential file (~/.aws/credentials)
    3. IAM role (for EC2/ECS/Lambda)

    Args:
        aws_access_key_id: AWS access key ID (optional)
        aws_secret_access_key: AWS secret access key (optional)
        region: AWS region

    Returns:
        boto3 S3 client
    """
    try:
        if aws_access_key_id and aws_secret_access_key:
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region,
            )
        else:
            # Use default credential chain (IAM roles, env vars, etc.)
            s3_client = boto3.client("s3", region_name=region)

        logger.info(f"Connected to S3 in region {region}")
        return s3_client
    except Exception as e:
        logger.error(f"Error connecting to S3: {e}")
        raise


def create_bucket_if_not_exists(s3_client, bucket_name: str, region: str) -> bool:
    """
    Create S3 bucket if it doesn't exist.

    Args:
        s3_client: boto3 S3 client
        bucket_name: Name of the bucket
        region: AWS region

    Returns:
        True if bucket exists or was created
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' already exists")
        return True
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "404":
            try:
                if region == "us-east-1":
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name, CreateBucketConfiguration={"LocationConstraint": region}
                    )
                logger.info(f"Bucket '{bucket_name}' created successfully")
                return True
            except ClientError as create_error:
                logger.error(f"Error creating bucket: {create_error}")
                raise
        else:
            logger.error(f"Error checking bucket: {e}")
            raise


def upload_file_to_s3(
    s3_client, local_file_path: str, bucket_name: str, s3_key: str = None, prefix: str = "raw/elasticsearch"
) -> str:
    """
    Upload a file to S3.

    Args:
        s3_client: boto3 S3 client
        local_file_path: Path to local file
        bucket_name: S3 bucket name
        s3_key: Full S3 key (optional, will be auto-generated if not provided)
        prefix: S3 prefix/folder path

    Returns:
        S3 URI of uploaded file

    Raises:
        S3UploadError: If upload fails
    """
    try:
        if s3_key is None:
            # Generate key with date partitioning
            date_partition = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            file_name = Path(local_file_path).name
            s3_key = f"{prefix}/{date_partition}/{file_name}"

        s3_client.upload_file(local_file_path, bucket_name, s3_key)

        s3_uri = f"s3://{bucket_name}/{s3_key}"
        logger.info(f"Uploaded {local_file_path} to {s3_uri}")
        return s3_uri

    except ClientError as e:
        raise S3UploadError(f"Failed to upload {local_file_path} to S3: {e}") from e


def list_s3_objects(s3_client, bucket_name: str, prefix: str = "") -> list:
    """
    List all objects in S3 bucket using pagination.

    Handles buckets with more than 1000 objects by using the paginator API.

    Args:
        s3_client: boto3 S3 client
        bucket_name: S3 bucket name
        prefix: Prefix to filter objects

    Returns:
        List of object keys
    """
    try:
        objects = []
        paginator = s3_client.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                objects.append(obj["Key"])

        logger.info(f"Found {len(objects)} objects in s3://{bucket_name}/{prefix}")
        return objects
    except ClientError as e:
        logger.error(f"Error listing S3 objects: {e}")
        raise
