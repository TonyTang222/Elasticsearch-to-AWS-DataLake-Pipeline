"""FastAPI application for the Elasticsearch-to-S3 Pipeline API."""

import logging
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.responses import JSONResponse

from api.models import ErrorResponse
from api.routes import data, health, pipelines
from api.run_store import RunStore
from utils.exceptions import (
    ConfigurationError,
    DataValidationError,
    ElasticsearchConnectionError,
    ElasticsearchExtractionError,
    PipelineError,
    S3UploadError,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown."""
    app.state.run_store = RunStore()
    app.state.executor = ThreadPoolExecutor(max_workers=2)
    logger.info("API started")
    yield
    app.state.executor.shutdown(wait=False)
    logger.info("API stopped")


app = FastAPI(
    title="Elasticsearch-to-S3 Pipeline API",
    description="REST API for triggering and monitoring ES-to-S3 ETL pipelines",
    version="1.0.0",
    lifespan=lifespan,
)

# Register routers
app.include_router(health.router)
app.include_router(pipelines.router, prefix="/api/v1")
app.include_router(data.router, prefix="/api/v1")


# Global exception handler for pipeline errors
_STATUS_MAP = {
    ElasticsearchConnectionError: 503,
    ElasticsearchExtractionError: 502,
    S3UploadError: 502,
    DataValidationError: 422,
    ConfigurationError: 500,
}


@app.exception_handler(PipelineError)
async def pipeline_error_handler(request, exc: PipelineError):
    status_code = _STATUS_MAP.get(type(exc), 500)
    return JSONResponse(
        status_code=status_code,
        content=ErrorResponse(
            error=type(exc).__name__,
            detail=str(exc),
            timestamp=datetime.now(timezone.utc),
        ).model_dump(mode="json"),
    )
