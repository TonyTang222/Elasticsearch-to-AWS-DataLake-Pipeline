"""Data query endpoints for S3 file listing, Parquet preview, and Iceberg table queries."""

import io
import logging

import pandas as pd
from fastapi import APIRouter, HTTPException, Query

from api.models import ErrorResponse, IcebergPreview, ParquetPreview, S3FileList
from etls.aws_etl import connect_to_s3, list_s3_objects
from etls.iceberg_etl import connect_catalog, get_snapshot_history, scan_table
from utils.constants import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    AWS_BUCKET_NAME,
    ICEBERG_CATALOG_NAME,
    ICEBERG_CATALOG_TYPE,
    ICEBERG_NAMESPACE,
    ICEBERG_TABLE_NAME,
    ICEBERG_WAREHOUSE_PATH,
)

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    "/data/files",
    response_model=S3FileList,
    responses={503: {"model": ErrorResponse}},
)
def list_s3_files(
    prefix: str = Query(default="bronze/elasticsearch", description="S3 prefix to list"),
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
    s3_key: str = Query(description="S3 object key (e.g. bronze/elasticsearch/XXXX-XX-XX/file.parquet)"),
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


def _load_iceberg_table():
    """Connect to Iceberg catalog and load the configured table."""
    catalog_properties = {"warehouse": ICEBERG_WAREHOUSE_PATH}
    if AWS_REGION:
        catalog_properties["client.region"] = AWS_REGION
    if AWS_ACCESS_KEY_ID:
        catalog_properties["client.access-key-id"] = AWS_ACCESS_KEY_ID
    if AWS_SECRET_ACCESS_KEY:
        catalog_properties["client.secret-access-key"] = AWS_SECRET_ACCESS_KEY

    catalog = connect_catalog(
        catalog_name=ICEBERG_CATALOG_NAME,
        catalog_type=ICEBERG_CATALOG_TYPE,
        **catalog_properties,
    )
    table_identifier = f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}"
    return catalog.load_table(table_identifier)


@router.get(
    "/data/iceberg/preview",
    response_model=IcebergPreview,
    responses={
        404: {"model": ErrorResponse},
        503: {"model": ErrorResponse},
    },
)
def preview_iceberg_table(
    limit: int = Query(default=50, ge=1, le=500, description="Max rows to return"),
    snapshot_id: int | None = Query(default=None, description="Snapshot ID for time-travel query"),
):
    """Preview rows from the Iceberg table (Silver layer)."""
    try:
        table = _load_iceberg_table()
        df = scan_table(table, limit=limit, snapshot_id=snapshot_id)

        return IcebergPreview(
            table_name=f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}",
            columns=list(df.columns),
            total_rows=len(df),
            limit=limit,
            snapshot_id=str(snapshot_id) if snapshot_id else None,
            rows=df.to_dict(orient="records"),
        )
    except Exception as e:
        logger.error(f"Failed to preview Iceberg table: {e}")
        if "NoSuchTableError" in type(e).__name__ or "not found" in str(e).lower():
            raise HTTPException(status_code=404, detail=str(e))
        raise HTTPException(status_code=503, detail=str(e))


@router.get(
    "/data/iceberg/snapshots",
    responses={503: {"model": ErrorResponse}},
)
def get_iceberg_snapshots():
    """Get snapshot history for the Iceberg table (for time-travel queries)."""
    try:
        table = _load_iceberg_table()
    except Exception as e:
        logger.error(f"Failed to load Iceberg table: {e}", exc_info=True)
        raise HTTPException(status_code=503, detail=str(e))

    try:
        history = get_snapshot_history(table)
    except Exception as e:
        logger.error(f"Failed to read snapshot history: {e}", exc_info=True)
        # Fallback: return basic info without detailed history
        try:
            current = table.current_snapshot()
            fallback = []
            if current:
                fallback.append(
                    {
                        "snapshot_id": str(current.snapshot_id),
                        "timestamp_ms": current.timestamp_ms,
                        "summary": {},
                    }
                )
            return {
                "table_name": f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}",
                "snapshots": fallback,
                "total": len(fallback),
                "warning": f"Partial result due to: {e}",
            }
        except Exception:
            raise HTTPException(status_code=503, detail=str(e))

    return {
        "table_name": f"{ICEBERG_NAMESPACE}.{ICEBERG_TABLE_NAME}",
        "snapshots": history,
        "total": len(history),
    }
