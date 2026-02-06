"""Tests for the FastAPI REST API layer."""

import time
from unittest.mock import patch, MagicMock

import pandas as pd


class TestHealthEndpoint:
    """Tests for GET /health."""

    def test_health_returns_200(self, test_client):
        with patch("api.routes.health.connect_elasticsearch"):
            response = test_client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert data["version"] == "1.0.0"
        assert "timestamp" in data

    def test_health_shows_elasticsearch_connected(self, test_client):
        with patch("api.routes.health.connect_elasticsearch") as mock_connect:
            mock_connect.return_value = MagicMock()
            response = test_client.get("/health")
        assert response.json()["elasticsearch"] == "connected"

    def test_health_shows_elasticsearch_unavailable(self, test_client):
        with patch("api.routes.health.connect_elasticsearch") as mock_connect:
            mock_connect.side_effect = ConnectionError("ES down")
            response = test_client.get("/health")
        assert response.status_code == 200
        assert response.json()["elasticsearch"] == "unavailable"


class TestTriggerPipeline:
    """Tests for POST /api/v1/pipelines/run."""

    def test_trigger_returns_202_with_run_id(self, test_client):
        with patch("api.routes.pipelines.elasticsearch_pipeline"), patch("api.routes.pipelines.upload_s3_pipeline"):
            response = test_client.post("/api/v1/pipelines/run", json={})
        assert response.status_code == 202
        data = response.json()
        assert "run_id" in data
        assert data["status"] == "pending"
        assert data["message"] == "Pipeline run queued"

    def test_trigger_with_custom_params(self, test_client):
        with patch("api.routes.pipelines.elasticsearch_pipeline"), patch("api.routes.pipelines.upload_s3_pipeline"):
            response = test_client.post(
                "/api/v1/pipelines/run",
                json={
                    "index_name": "my-index",
                    "upload_to_s3": False,
                    "time_range_hours": 48,
                },
            )
        assert response.status_code == 202

    def test_trigger_invalid_time_range_zero(self, test_client):
        response = test_client.post(
            "/api/v1/pipelines/run",
            json={
                "time_range_hours": 0,
            },
        )
        assert response.status_code == 422

    def test_trigger_invalid_time_range_too_large(self, test_client):
        response = test_client.post(
            "/api/v1/pipelines/run",
            json={
                "time_range_hours": 1000,
            },
        )
        assert response.status_code == 422

    def test_trigger_creates_run_in_store(self, test_client):
        with (
            patch("api.routes.pipelines.elasticsearch_pipeline") as mock_es,
            patch("api.routes.pipelines.upload_s3_pipeline") as mock_s3,
        ):
            mock_es.return_value = "/tmp/output.parquet"
            mock_s3.return_value = "s3://bucket/output.parquet"

            response = test_client.post("/api/v1/pipelines/run", json={})
            run_id = response.json()["run_id"]

            time.sleep(0.5)

            detail = test_client.get(f"/api/v1/pipelines/runs/{run_id}")
        assert detail.status_code == 200
        assert detail.json()["run_id"] == run_id


class TestPipelineRunStatus:
    """Tests for GET /api/v1/pipelines/runs and /runs/{run_id}."""

    def test_list_runs_empty(self, test_client):
        response = test_client.get("/api/v1/pipelines/runs")
        assert response.status_code == 200
        data = response.json()
        assert data["runs"] == []
        assert data["total"] == 0

    def test_list_runs_returns_created_runs(self, test_client):
        with patch("api.routes.pipelines.elasticsearch_pipeline"), patch("api.routes.pipelines.upload_s3_pipeline"):
            test_client.post("/api/v1/pipelines/run", json={})
            test_client.post("/api/v1/pipelines/run", json={})

        response = test_client.get("/api/v1/pipelines/runs")
        assert response.json()["total"] == 2

    def test_list_runs_with_status_filter(self, test_client):
        store = test_client.app.state.run_store
        store.create("run-1", {})
        store.update("run-1", status="completed")
        store.create("run-2", {})
        store.update("run-2", status="failed")

        response = test_client.get("/api/v1/pipelines/runs?status=completed")
        data = response.json()
        assert data["total"] == 1
        assert data["runs"][0]["status"] == "completed"

    def test_get_run_detail(self, test_client):
        store = test_client.app.state.run_store
        store.create("test-run-id", {"output_format": "parquet"})
        store.update("test-run-id", status="completed", output_file="/tmp/test.parquet")

        response = test_client.get("/api/v1/pipelines/runs/test-run-id")
        assert response.status_code == 200
        data = response.json()
        assert data["run_id"] == "test-run-id"
        assert data["status"] == "completed"
        assert data["output_file"] == "/tmp/test.parquet"

    def test_get_run_not_found(self, test_client):
        response = test_client.get("/api/v1/pipelines/runs/nonexistent-id")
        assert response.status_code == 404


class TestPipelineExecution:
    """Tests for background pipeline execution."""

    def test_pipeline_run_completes_successfully(self, test_client):
        with (
            patch("api.routes.pipelines.elasticsearch_pipeline") as mock_es,
            patch("api.routes.pipelines.upload_s3_pipeline") as mock_s3,
        ):
            mock_es.return_value = "/tmp/output.parquet"
            mock_s3.return_value = "s3://bucket/bronze/output.parquet"

            response = test_client.post("/api/v1/pipelines/run", json={})
            run_id = response.json()["run_id"]

            # Wait for background thread to complete
            time.sleep(0.5)

            detail = test_client.get(f"/api/v1/pipelines/runs/{run_id}")
            data = detail.json()
            assert data["status"] == "completed"
            assert data["output_file"] == "/tmp/output.parquet"
            assert data["s3_uri"] == "s3://bucket/bronze/output.parquet"
            assert data["duration_seconds"] is not None

    def test_pipeline_run_fails_on_error(self, test_client):
        with (
            patch("api.routes.pipelines.elasticsearch_pipeline") as mock_es,
            patch("api.routes.pipelines.upload_s3_pipeline"),
        ):
            mock_es.side_effect = ConnectionError("ES unreachable")

            response = test_client.post("/api/v1/pipelines/run", json={})
            run_id = response.json()["run_id"]

            time.sleep(0.5)

            detail = test_client.get(f"/api/v1/pipelines/runs/{run_id}")
            data = detail.json()
            assert data["status"] == "failed"
            assert "ES unreachable" in data["error"]

    def test_pipeline_run_skips_s3_when_disabled(self, test_client):
        with (
            patch("api.routes.pipelines.elasticsearch_pipeline") as mock_es,
            patch("api.routes.pipelines.upload_s3_pipeline") as mock_s3,
        ):
            mock_es.return_value = "/tmp/output.parquet"

            response = test_client.post(
                "/api/v1/pipelines/run",
                json={
                    "upload_to_s3": False,
                },
            )
            run_id = response.json()["run_id"]

            time.sleep(0.5)

            detail = test_client.get(f"/api/v1/pipelines/runs/{run_id}")
            data = detail.json()
            assert data["status"] == "completed"
            assert data["s3_uri"] is None
            mock_s3.assert_not_called()


class TestDataEndpoints:
    """Tests for GET /api/v1/data/files."""

    def test_list_s3_files(self, test_client):
        with (
            patch("api.routes.data.connect_to_s3") as mock_connect,
            patch("api.routes.data.list_s3_objects") as mock_list,
        ):
            mock_connect.return_value = MagicMock()
            mock_list.return_value = [
                "bronze/elasticsearch/2025-01-28/file1.parquet",
                "bronze/elasticsearch/2025-01-29/file2.parquet",
            ]

            response = test_client.get("/api/v1/data/files")
            assert response.status_code == 200
            data = response.json()
            assert data["total"] == 2
            assert len(data["files"]) == 2

    def test_list_s3_files_with_prefix(self, test_client):
        with patch("api.routes.data.connect_to_s3"), patch("api.routes.data.list_s3_objects") as mock_list:
            mock_list.return_value = []

            response = test_client.get("/api/v1/data/files?prefix=bronze/custom")
            assert response.status_code == 200
            mock_list.assert_called_once()
            assert mock_list.call_args[0][2] == "bronze/custom"

    def test_list_s3_files_s3_unavailable(self, test_client):
        with patch("api.routes.data.connect_to_s3") as mock_connect:
            mock_connect.side_effect = Exception("S3 unreachable")

            response = test_client.get("/api/v1/data/files")
            assert response.status_code == 503


class TestParquetPreview:
    """Tests for GET /api/v1/data/preview."""

    def test_preview_parquet_success(self, test_client):
        mock_df = pd.DataFrame(
            {
                "id": ["1", "2", "3"],
                "message": ["msg1", "msg2", "msg3"],
            }
        )
        mock_s3 = MagicMock()
        mock_body = MagicMock()
        mock_body.read.return_value = b"fake-parquet-bytes"
        mock_s3.get_object.return_value = {"Body": mock_body}

        with (
            patch("api.routes.data.connect_to_s3", return_value=mock_s3),
            patch("api.routes.data.pd.read_parquet", return_value=mock_df),
        ):
            url = "/api/v1/data/preview?s3_key=bronze/elasticsearch/XXXX-XX-XX/test.parquet&limit=2"
            response = test_client.get(url)
        assert response.status_code == 200
        data = response.json()
        assert data["total_rows"] == 3
        assert len(data["rows"]) == 2
        assert data["columns"] == ["id", "message"]
        assert data["file_path"].startswith("s3://")

    def test_preview_s3_key_not_found(self, test_client):
        mock_s3 = MagicMock()
        error_response = {"Error": {"Code": "NoSuchKey", "Message": "Not found"}}
        mock_s3.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})
        mock_s3.get_object.side_effect = mock_s3.exceptions.NoSuchKey(error_response, "GetObject")

        with patch("api.routes.data.connect_to_s3", return_value=mock_s3):
            response = test_client.get("/api/v1/data/preview?s3_key=bronze/nonexistent.parquet")
        assert response.status_code == 404

    def test_preview_invalid_extension(self, test_client):
        response = test_client.get("/api/v1/data/preview?s3_key=bronze/data.txt")
        assert response.status_code == 422


class TestRunStore:
    """Unit tests for RunStore."""

    def test_create_run(self, run_store):
        run = run_store.create("test-1", {"format": "parquet"})
        assert run.run_id == "test-1"
        assert run.status == "pending"

    def test_update_run(self, run_store):
        run_store.create("test-1", {})
        updated = run_store.update("test-1", status="running")
        assert updated.status == "running"

    def test_update_nonexistent_returns_none(self, run_store):
        result = run_store.update("nonexistent", status="running")
        assert result is None

    def test_get_run(self, run_store):
        run_store.create("test-1", {"key": "value"})
        run = run_store.get("test-1")
        assert run is not None
        assert run.config == {"key": "value"}

    def test_get_nonexistent_returns_none(self, run_store):
        assert run_store.get("nonexistent") is None

    def test_list_runs_sorted_by_created_at(self, run_store):
        run_store.create("run-1", {})
        run_store.create("run-2", {})
        runs = run_store.list_runs()
        assert runs[0].run_id == "run-2"  # Most recent first

    def test_list_runs_with_limit(self, run_store):
        for i in range(5):
            run_store.create(f"run-{i}", {})
        runs = run_store.list_runs(limit=3)
        assert len(runs) == 3

    def test_list_runs_filter_by_status(self, run_store):
        run_store.create("run-1", {})
        run_store.update("run-1", status="completed")
        run_store.create("run-2", {})

        completed = run_store.list_runs(status="completed")
        assert len(completed) == 1
        assert completed[0].run_id == "run-1"


class TestIcebergPreview:
    """Tests for GET /api/v1/data/iceberg/preview."""

    def test_preview_iceberg_success(self, test_client):
        mock_table = MagicMock()
        mock_df = pd.DataFrame({"id": ["1", "2", "3"], "message": ["a", "b", "c"]})

        with (
            patch("api.routes.data._load_iceberg_table", return_value=mock_table),
            patch("api.routes.data.scan_table", return_value=mock_df),
        ):
            response = test_client.get("/api/v1/data/iceberg/preview?limit=50")

        assert response.status_code == 200
        data = response.json()
        assert data["columns"] == ["id", "message"]
        assert data["total_rows"] == 3
        assert data["limit"] == 50
        assert len(data["rows"]) == 3
        assert data["snapshot_id"] is None

    def test_preview_iceberg_with_snapshot_id(self, test_client):
        mock_table = MagicMock()
        mock_df = pd.DataFrame({"id": ["1"], "message": ["old"]})

        with (
            patch("api.routes.data._load_iceberg_table", return_value=mock_table),
            patch("api.routes.data.scan_table", return_value=mock_df) as mock_scan,
        ):
            response = test_client.get("/api/v1/data/iceberg/preview?snapshot_id=1234567890")

        assert response.status_code == 200
        data = response.json()
        assert data["snapshot_id"] == "1234567890"
        mock_scan.assert_called_once_with(mock_table, limit=50, snapshot_id=1234567890)

    def test_preview_iceberg_table_not_found(self, test_client):
        from pyiceberg.exceptions import NoSuchTableError

        with patch("api.routes.data._load_iceberg_table") as mock_load:
            mock_load.side_effect = NoSuchTableError("datalake.elasticsearch_logs")

            response = test_client.get("/api/v1/data/iceberg/preview")

        assert response.status_code == 404

    def test_preview_iceberg_catalog_unavailable(self, test_client):
        with patch("api.routes.data._load_iceberg_table") as mock_load:
            mock_load.side_effect = Exception("Catalog unreachable")

            response = test_client.get("/api/v1/data/iceberg/preview")

        assert response.status_code == 503


class TestIcebergSnapshots:
    """Tests for GET /api/v1/data/iceberg/snapshots."""

    def test_get_snapshots_success(self, test_client):
        mock_table = MagicMock()
        mock_history = [
            {
                "snapshot_id": "1234567890",
                "timestamp_ms": 1706400000000,
                "summary": {"added-data-files": "1", "added-records": "100"},
            },
            {
                "snapshot_id": "9876543210",
                "timestamp_ms": 1706486400000,
                "summary": {"added-data-files": "2", "added-records": "200"},
            },
        ]

        with (
            patch("api.routes.data._load_iceberg_table", return_value=mock_table),
            patch("api.routes.data.get_snapshot_history", return_value=mock_history),
        ):
            response = test_client.get("/api/v1/data/iceberg/snapshots")

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        assert len(data["snapshots"]) == 2
        assert data["snapshots"][0]["snapshot_id"] == "1234567890"
        assert "table_name" in data

    def test_get_snapshots_empty(self, test_client):
        mock_table = MagicMock()

        with (
            patch("api.routes.data._load_iceberg_table", return_value=mock_table),
            patch("api.routes.data.get_snapshot_history", return_value=[]),
        ):
            response = test_client.get("/api/v1/data/iceberg/snapshots")

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 0
        assert data["snapshots"] == []

    def test_get_snapshots_catalog_unavailable(self, test_client):
        with patch("api.routes.data._load_iceberg_table") as mock_load:
            mock_load.side_effect = Exception("Catalog unreachable")

            response = test_client.get("/api/v1/data/iceberg/snapshots")

        assert response.status_code == 503
