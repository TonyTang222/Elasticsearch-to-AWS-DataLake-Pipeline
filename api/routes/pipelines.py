"""Pipeline trigger and status endpoints."""

import uuid
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import get_executor, get_run_store
from api.models import (
    ErrorResponse,
    PipelineRunDetail,
    PipelineRunListResponse,
    PipelineRunRequest,
    PipelineRunResponse,
    PipelineRunSummary,
)
from api.run_store import RunStore
from pipelines.elasticsearch_pipeline import elasticsearch_pipeline
from pipelines.aws_s3_pipeline import upload_s3_pipeline
from pipelines.iceberg_pipeline import iceberg_pipeline

logger = logging.getLogger(__name__)
router = APIRouter()


def _execute_pipeline(store: RunStore, run_id: str, config: PipelineRunRequest):
    """Run pipeline in background thread."""
    store.update(run_id, status="running")
    try:
        file_name = f"pipeline_{run_id[:8]}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

        output_file = elasticsearch_pipeline(
            file_name=file_name,
            index_name=config.index_name,
            time_range_hours=config.time_range_hours,
            use_time_filter=config.use_time_filter,
        )

        s3_uri = None
        if config.upload_to_s3:
            s3_uri = upload_s3_pipeline(
                file_path=output_file,
                s3_prefix=config.s3_prefix,
            )

        iceberg_snapshot_id = None
        if config.write_to_iceberg:
            import pandas as pd

            df = pd.read_parquet(output_file)
            iceberg_result = iceberg_pipeline(df=df)
            iceberg_snapshot_id = iceberg_result.get("snapshot_id")

        store.update(
            run_id,
            status="completed",
            output_file=output_file,
            s3_uri=s3_uri,
            iceberg_snapshot_id=iceberg_snapshot_id,
            completed_at=datetime.now(timezone.utc),
        )
        logger.info(f"Pipeline run {run_id} completed successfully")

    except Exception as e:
        logger.error(f"Pipeline run {run_id} failed: {e}")
        store.update(
            run_id,
            status="failed",
            error=str(e),
            completed_at=datetime.now(timezone.utc),
        )


@router.post(
    "/pipelines/run",
    response_model=PipelineRunResponse,
    status_code=202,
    responses={422: {"model": ErrorResponse}},
)
def trigger_pipeline(
    request: PipelineRunRequest,
    store: RunStore = Depends(get_run_store),
    executor: ThreadPoolExecutor = Depends(get_executor),
):
    """Trigger a new pipeline run. Returns immediately with a run ID for status polling."""
    run_id = str(uuid.uuid4())
    run = store.create(run_id, request.model_dump())
    executor.submit(_execute_pipeline, store, run_id, request)

    return PipelineRunResponse(
        run_id=run_id,
        status="pending",
        message="Pipeline run queued",
        created_at=run.created_at,
    )


@router.get("/pipelines/runs", response_model=PipelineRunListResponse)
def list_pipeline_runs(
    status: str | None = Query(default=None, description="Filter by status"),
    limit: int = Query(default=20, ge=1, le=100, description="Max runs to return"),
    store: RunStore = Depends(get_run_store),
):
    """List all pipeline runs, optionally filtered by status."""
    runs = store.list_runs(status=status, limit=limit)
    return PipelineRunListResponse(
        runs=[
            PipelineRunSummary(
                run_id=r.run_id,
                status=r.status,
                created_at=r.created_at,
                completed_at=r.completed_at,
            )
            for r in runs
        ],
        total=len(runs),
    )


@router.get(
    "/pipelines/runs/{run_id}",
    response_model=PipelineRunDetail,
    responses={404: {"model": ErrorResponse}},
)
def get_pipeline_run(
    run_id: str,
    store: RunStore = Depends(get_run_store),
):
    """Get detailed status of a specific pipeline run."""
    run = store.get(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail=f"Run '{run_id}' not found")

    duration = None
    if run.completed_at and run.created_at:
        duration = (run.completed_at - run.created_at).total_seconds()

    return PipelineRunDetail(
        run_id=run.run_id,
        status=run.status,
        created_at=run.created_at,
        completed_at=run.completed_at,
        config=run.config,
        output_file=run.output_file,
        s3_uri=run.s3_uri,
        iceberg_snapshot_id=run.iceberg_snapshot_id,
        error=run.error,
        duration_seconds=duration,
    )
