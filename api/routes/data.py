"""Data query endpoints for S3 file listing and Parquet preview."""

import io
import logging

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from api.models import ErrorResponse, ParquetPreview, S3FileList
from etls.aws_etl import connect_to_s3, list_s3_objects
from utils.constants import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, AWS_BUCKET_NAME

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    "/data/files",
    response_model=S3FileList,
    responses={503: {"model": ErrorResponse}},
)
def list_s3_files(
    prefix: str = Query(default="raw/elasticsearch", description="S3 prefix to list"),
    limit: int = Query(default=50, ge=1, le=500, description="Max files to return"),
):
    """List files in the S3 data lake bucket."""
    try:
        s3_client = connect_to_s3(
            aws_access_key_id=AWS_ACCESS_KEY_ID if AWS_ACCESS_KEY_ID else None,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY if AWS_SECRET_ACCESS_KEY else None,
            region=AWS_REGION,
        )
        files = list_s3_objects(s3_client, AWS_BUCKET_NAME, prefix)
        files = files[:limit]

        return S3FileList(
            bucket=AWS_BUCKET_NAME,
            prefix=prefix,
            files=files,
            total=len(files),
        )
    except Exception as e:
        logger.error(f"Failed to list S3 files: {e}")
        raise HTTPException(
            status_code=503,
            detail=str(e),
        )


@router.get(
    "/data/preview",
    response_model=ParquetPreview,
    responses={404: {"model": ErrorResponse}, 422: {"model": ErrorResponse}},
)
def preview_parquet(
    s3_key: str = Query(description="S3 object key (e.g. raw/elasticsearch/XXXX-XX-XX/file.parquet)"),
    limit: int = Query(default=50, ge=1, le=500, description="Max rows to return"),
):
    """Preview contents of a Parquet file from S3 as JSON."""
    if not s3_key.endswith(".parquet"):
        raise HTTPException(status_code=422, detail="Only .parquet files are supported")

    try:
        s3_client = connect_to_s3(
            aws_access_key_id=AWS_ACCESS_KEY_ID if AWS_ACCESS_KEY_ID else None,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY if AWS_SECRET_ACCESS_KEY else None,
            region=AWS_REGION,
        )

        response = s3_client.get_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
        parquet_bytes = response["Body"].read()

        df = pd.read_parquet(io.BytesIO(parquet_bytes))
        total_rows = len(df)
        rows = df.head(limit).to_dict(orient="records")

        return ParquetPreview(
            file_path=f"s3://{AWS_BUCKET_NAME}/{s3_key}",
            columns=list(df.columns),
            total_rows=total_rows,
            limit=limit,
            rows=rows,
        )
    except s3_client.exceptions.NoSuchKey:
        raise HTTPException(status_code=404, detail=f"S3 key not found: {s3_key}")
    except Exception as e:
        logger.error(f"Failed to preview Parquet file from S3: {e}")
        raise HTTPException(status_code=500, detail=str(e))
