"""Tests for the HTTP backend."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from requests.exceptions import ConnectionError as ReqConnectionError

from pipewatch.backends.http import HTTPBackend, _resolve_json_path
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend() -> HTTPBackend:
    return HTTPBackend(config={"timeout": 5})


def _pipeline(extras: dict | None = None, name: str = "my_pipeline"):
    return SimpleNamespace(name=name, extras=extras or {})


def _mock_response(status_code: int = 200, json_data: dict | None = None) -> MagicMock:
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    return resp


# ---------------------------------------------------------------------------
# Happy-path
# ---------------------------------------------------------------------------

def test_healthy_on_200(backend):
    pipeline = _pipeline({"url": "http://example.com/health"})
    with patch("pipewatch.backends.http.requests.get", return_value=_mock_response(200)):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_healthy_with_truthy_json_path(backend):
    pipeline = _pipeline({"url": "http://example.com/health", "json_path": "data.ok"})
    resp = _mock_response(200, json_data={"data": {"ok": True}})
    with patch("pipewatch.backends.http.requests.get", return_value=resp):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.HEALTHY


# ---------------------------------------------------------------------------
# Failure cases
# ---------------------------------------------------------------------------

def test_failed_on_unexpected_status(backend):
    pipeline = _pipeline({"url": "http://example.com/health", "expected_status": 200})
    with patch("pipewatch.backends.http.requests.get", return_value=_mock_response(500)):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.FAILED
    assert "500" in result.message


def test_failed_when_json_path_is_falsy(backend):
    pipeline = _pipeline({"url": "http://example.com/health", "json_path": "status"})
    resp = _mock_response(200, json_data={"status": 0})
    with patch("pipewatch.backends.http.requests.get", return_value=resp):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.FAILED
    assert "status" in result.message


# ---------------------------------------------------------------------------
# Unknown / error cases
# ---------------------------------------------------------------------------

def test_unknown_when_no_url_configured(backend):
    pipeline = _pipeline(extras={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_unknown_on_request_exception(backend):
    pipeline = _pipeline({"url": "http://unreachable.example.com/health"})
    with patch(
        "pipewatch.backends.http.requests.get",
        side_effect=ReqConnectionError("connection refused"),
    ):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_unknown_when_json_path_missing_key(backend):
    pipeline = _pipeline({"url": "http://example.com/health", "json_path": "missing.key"})
    resp = _mock_response(200, json_data={"other": 1})
    with patch("pipewatch.backends.http.requests.get", return_value=resp):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------

def test_resolve_json_path_nested():
    data = {"a": {"b": {"c": 42}}}
    assert _resolve_json_path(data, "a.b.c") == 42


def test_resolve_json_path_raises_on_non_dict():
    with pytest.raises(TypeError):
        _resolve_json_path({"a": [1, 2, 3]}, "a.0")
