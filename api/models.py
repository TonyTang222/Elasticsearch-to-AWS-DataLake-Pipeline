"""Pydantic models for API request/response validation."""

from datetime import datetime
from pydantic import BaseModel, field_validator


# ============== Request Models ==============


class PipelineRunRequest(BaseModel):
    """Request body for triggering a pipeline run."""

    index_name: str | None = None
    output_format: str = "parquet"
    upload_to_s3: bool = True
    time_range_hours: int = 24
    use_time_filter: bool = True
    s3_prefix: str = "raw/elasticsearch"

    @field_validator("output_format")
    @classmethod
    def validate_output_format(cls, v):
        if v not in ("csv", "parquet"):
            raise ValueError("output_format must be 'csv' or 'parquet'")
        return v

    @field_validator("time_range_hours")
    @classmethod
    def validate_time_range(cls, v):
        if v < 1 or v > 720:
            raise ValueError("time_range_hours must be between 1 and 720")
        return v


# ============== Response Models ==============


class HealthResponse(BaseModel):
    status: str
    version: str
    elasticsearch: str
    timestamp: datetime


class PipelineRunResponse(BaseModel):
    run_id: str
    status: str
    message: str
    created_at: datetime


class PipelineRunSummary(BaseModel):
    run_id: str
    status: str
    created_at: datetime
    completed_at: datetime | None = None


class PipelineRunListResponse(BaseModel):
    runs: list[PipelineRunSummary]
    total: int


class PipelineRunDetail(BaseModel):
    run_id: str
    status: str
    created_at: datetime
    completed_at: datetime | None = None
    config: dict
    output_file: str | None = None
    s3_uri: str | None = None
    error: str | None = None
    duration_seconds: float | None = None


class S3FileList(BaseModel):
    bucket: str
    prefix: str
    files: list[str]
    total: int


class ParquetPreview(BaseModel):
    file_path: str
    columns: list[str]
    total_rows: int
    limit: int
    rows: list[dict]


class ErrorResponse(BaseModel):
    error: str
    detail: str
    timestamp: datetime
