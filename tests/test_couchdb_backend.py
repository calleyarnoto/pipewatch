"""Tests for the CouchDB backend."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.couchdb import CouchDBBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend() -> CouchDBBackend:
    return CouchDBBackend(
        {"url": "http://localhost:5984", "username": "admin", "password": "secret"}
    )


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(
        name="my_pipeline",
        extra={"database": "etl_db", "threshold": 10},
    )


def _mock_response(json_data: dict, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    return resp


def test_healthy_when_count_meets_threshold(backend, _pipeline):
    resp = _mock_response({"doc_count": 15})
    with patch("pipewatch.backends.couchdb.requests.get", return_value=resp):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
    assert result.pipeline_name == "my_pipeline"


def test_failed_when_count_below_threshold(backend, _pipeline):
    resp = _mock_response({"doc_count": 3})
    with patch("pipewatch.backends.couchdb.requests.get", return_value=resp):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED
    assert "3" in result.message


def test_default_threshold_is_one(backend):
    pipeline = SimpleNamespace(name="p", extra={"database": "db"})
    resp = _mock_response({"doc_count": 1})
    with patch("pipewatch.backends.couchdb.requests.get", return_value=resp):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_unknown_when_database_missing(backend):
    pipeline = SimpleNamespace(name="p", extra={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "database" in result.message


def test_unknown_on_request_exception(backend, _pipeline):
    import requests

    with patch(
        "pipewatch.backends.couchdb.requests.get",
        side_effect=requests.RequestException("connection refused"),
    ):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_view_query_uses_total_rows(backend):
    pipeline = SimpleNamespace(
        name="p",
        extra={"database": "db", "design": "etl", "view": "counts", "threshold": 5},
    )
    resp = _mock_response({"total_rows": 7})
    with patch("pipewatch.backends.couchdb.requests.get", return_value=resp) as mock_get:
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.HEALTHY
    called_url = mock_get.call_args[0][0]
    assert "_design/etl/_view/counts" in called_url


def test_backend_is_registered():
    import importlib
    import pipewatch.backends.couchdb_register  # noqa: F401
    from pipewatch.backends import get_backend_class

    cls = get_backend_class("couchdb")
    assert cls is CouchDBBackend
