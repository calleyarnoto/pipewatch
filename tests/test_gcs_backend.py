"""Tests for the GCS backend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.gcs import GCSBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend() -> GCSBackend:
    return GCSBackend(config={"project": "my-project"})


@pytest.fixture()
def _pipeline() -> SimpleNamespace:
    return SimpleNamespace(
        name="gcs_pipeline",
        extra={"bucket": "my-bucket", "prefix": "data/", "threshold": "2"},
    )


def _make_blobs(n: int) -> list[MagicMock]:
    return [MagicMock() for _ in range(n)]


def test_healthy_when_count_meets_threshold(backend, _pipeline):
    with patch("pipewatch.backends.gcs.storage") as mock_storage:
        mock_client = MagicMock()
        mock_storage.Client.return_value = mock_client
        mock_client.list_blobs.return_value = _make_blobs(3)

        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.HEALTHY
    assert "3" in result.message


def test_failed_when_count_below_threshold(backend, _pipeline):
    with patch("pipewatch.backends.gcs.storage") as mock_storage:
        mock_client = MagicMock()
        mock_storage.Client.return_value = mock_client
        mock_client.list_blobs.return_value = _make_blobs(1)

        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.FAILED


def test_default_threshold_is_one(backend):
    pipeline = SimpleNamespace(
        name="gcs_default",
        extra={"bucket": "my-bucket"},
    )
    with patch("pipewatch.backends.gcs.storage") as mock_storage:
        mock_client = MagicMock()
        mock_storage.Client.return_value = mock_client
        mock_client.list_blobs.return_value = _make_blobs(1)

        result = backend.check_pipeline(pipeline)

    assert result.status == PipelineStatus.HEALTHY


def test_unknown_on_missing_bucket_key(backend):
    pipeline = SimpleNamespace(name="no_bucket", extra={})
    with patch("pipewatch.backends.gcs.storage"):
        result = backend.check_pipeline(pipeline)

    assert result.status == PipelineStatus.UNKNOWN
    assert "bucket" in result.message


def test_unknown_on_exception(backend, _pipeline):
    with patch("pipewatch.backends.gcs.storage") as mock_storage:
        mock_client = MagicMock()
        mock_storage.Client.return_value = mock_client
        mock_client.list_blobs.side_effect = RuntimeError("network error")

        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.UNKNOWN
    assert "network error" in result.message


def test_pipeline_name_in_result(backend, _pipeline):
    with patch("pipewatch.backends.gcs.storage") as mock_storage:
        mock_client = MagicMock()
        mock_storage.Client.return_value = mock_client
        mock_client.list_blobs.return_value = _make_blobs(5)

        result = backend.check_pipeline(_pipeline)

    assert result.pipeline_name == "gcs_pipeline"
