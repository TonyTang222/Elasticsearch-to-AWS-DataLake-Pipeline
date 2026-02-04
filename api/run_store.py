"""In-memory pipeline run tracking with thread-safe operations."""

import threading
from datetime import datetime, timezone
from dataclasses import dataclass, field


@dataclass
class PipelineRun:
    """Represents a single pipeline execution."""

    run_id: str
    status: str  # pending | running | completed | failed
    config: dict
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    output_file: str | None = None
    s3_uri: str | None = None
    iceberg_snapshot_id: str | None = None
    error: str | None = None


class RunStore:
    """Thread-safe in-memory store for pipeline runs."""

    def __init__(self):
        self._runs: dict[str, PipelineRun] = {}
        self._lock = threading.Lock()

    def create(self, run_id: str, config: dict) -> PipelineRun:
        with self._lock:
            run = PipelineRun(run_id=run_id, status="pending", config=config)
            self._runs[run_id] = run
            return run

    def update(self, run_id: str, **kwargs) -> PipelineRun | None:
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                return None
            for key, value in kwargs.items():
                if hasattr(run, key):
                    setattr(run, key, value)
            return run

    def get(self, run_id: str) -> PipelineRun | None:
        with self._lock:
            return self._runs.get(run_id)

    def list_runs(self, status: str | None = None, limit: int = 20) -> list[PipelineRun]:
        with self._lock:
            runs = list(self._runs.values())
            if status:
                runs = [r for r in runs if r.status == status]
            runs.sort(key=lambda r: r.created_at, reverse=True)
            return runs[:limit]
