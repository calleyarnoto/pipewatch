"""Tests for the Azure Blob Storage backend."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.azure_blob import AzureBlobBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend() -> AzureBlobBackend:
    return AzureBlobBackend({"connection_string": "DefaultEndpointsProtocol=https;..."})


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(
        name="my_blob_pipeline",
        config={"container": "data-lake", "prefix": "exports/", "threshold": "2"},
    )


def _make_container_mock(blob_names: list[str]) -> MagicMock:
    service_mock = MagicMock()
    container_mock = MagicMock()
    service_mock.get_container_client.return_value = container_mock
    container_mock.list_blobs.return_value = [
        SimpleNamespace(name=n) for n in blob_names
    ]
    return service_mock


def test_healthy_when_count_meets_threshold(backend, _pipeline):
    service_mock = _make_container_mock(["exports/a.parquet", "exports/b.parquet"])
    with patch(
        "pipewatch.backends.azure_blob.BlobServiceClient"
    ) as bsc_cls:
        bsc_cls.from_connection_string.return_value = service_mock
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.HEALTHY
    assert "2" in result.message


def test_failed_when_count_below_threshold(backend, _pipeline):
    service_mock = _make_container_mock(["exports/a.parquet"])
    with patch(
        "pipewatch.backends.azure_blob.BlobServiceClient"
    ) as bsc_cls:
        bsc_cls.from_connection_string.return_value = service_mock
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.FAILED
    assert "1" in result.message


def test_default_threshold_is_one(backend):
    pipeline = SimpleNamespace(
        name="default_threshold",
        config={"container": "raw"},
    )
    service_mock = _make_container_mock(["file.csv"])
    with patch(
        "pipewatch.backends.azure_blob.BlobServiceClient"
    ) as bsc_cls:
        bsc_cls.from_connection_string.return_value = service_mock
        result = backend.check_pipeline(pipeline)

    assert result.status == PipelineStatus.HEALTHY


def test_unknown_when_container_missing(backend):
    pipeline = SimpleNamespace(name="no_container", config={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "container" in result.message


def test_unknown_on_exception(backend, _pipeline):
    with patch(
        "pipewatch.backends.azure_blob.BlobServiceClient"
    ) as bsc_cls:
        bsc_cls.from_connection_string.side_effect = RuntimeError("auth failed")
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.UNKNOWN
    assert "auth failed" in result.message


def test_unknown_when_package_missing(backend, _pipeline, monkeypatch):
    import builtins
    real_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "azure.storage.blob":
            raise ImportError("No module named 'azure'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", mock_import)
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "azure-storage-blob" in result.message
