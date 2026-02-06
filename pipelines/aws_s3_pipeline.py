import os
import glob
import logging

from etls.aws_etl import connect_to_s3, create_bucket_if_not_exists, upload_file_to_s3
from utils.constants import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_BUCKET_NAME, OUTPUT_PATH

logger = logging.getLogger(__name__)


def upload_s3_pipeline(file_path: str = None, s3_prefix: str = "bronze/elasticsearch") -> str:
    """
    Upload extracted data to S3.

    Args:
        file_path: Specific file to upload (if None, uploads latest from OUTPUT_PATH)
        s3_prefix: S3 prefix/folder for uploaded files

    Returns:
        S3 URI of uploaded file

    Raises:
        FileNotFoundError: If no file is found to upload
    """
    logger.info("Starting S3 upload pipeline")

    # Connect to S3 (uses IAM roles if credentials are empty)
    s3_client = connect_to_s3(
        aws_access_key_id=AWS_ACCESS_KEY_ID if AWS_ACCESS_KEY_ID else None,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY if AWS_SECRET_ACCESS_KEY else None,
        region=AWS_REGION,
    )

    # Ensure bucket exists
    create_bucket_if_not_exists(s3_client, AWS_BUCKET_NAME, AWS_REGION)

    # Find file to upload
    if file_path is None:
        # Get the latest output file from output directory
        output_files = glob.glob(f"{OUTPUT_PATH}/*.parquet")
        if not output_files:
            raise FileNotFoundError(f"No output files found in {OUTPUT_PATH}")
        file_path = max(output_files, key=os.path.getctime)
        logger.info(f"Found latest file: {file_path}")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    # Upload to S3
    s3_uri = upload_file_to_s3(
        s3_client=s3_client, local_file_path=file_path, bucket_name=AWS_BUCKET_NAME, prefix=s3_prefix
    )

    logger.info(f"S3 upload pipeline completed. File uploaded to: {s3_uri}")
    return s3_uri


if __name__ == "__main__":
    result = upload_s3_pipeline()
    print(f"Uploaded to: {result}")
