"""Tests for the Datadog backend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.datadog import DatadogBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    return DatadogBackend({"api_key": "test-api", "app_key": "test-app"})


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(
        name="orders_pipeline",
        options={"query": "avg:custom.orders.count{*}", "threshold": "10"},
    )


def _mock_response(pointlist, status_code=200):
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = {"series": [{"pointlist": pointlist}]}
    mock.raise_for_status = MagicMock()
    return mock


def test_healthy_when_value_meets_threshold(backend, _pipeline):
    with patch("pipewatch.backends.datadog.requests.get") as mock_get:
        mock_get.return_value = _mock_response([[1700000000000, 42.0]])
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_failed_when_value_below_threshold(backend, _pipeline):
    with patch("pipewatch.backends.datadog.requests.get") as mock_get:
        mock_get.return_value = _mock_response([[1700000000000, 3.0]])
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_unknown_when_no_series(backend, _pipeline):
    with patch("pipewatch.backends.datadog.requests.get") as mock_get:
        mock = MagicMock()
        mock.json.return_value = {"series": []}
        mock.raise_for_status = MagicMock()
        mock_get.return_value = mock
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_unknown_when_no_query():
    b = DatadogBackend({"api_key": "k", "app_key": "a"})
    pipeline = SimpleNamespace(name="p", options={})
    result = b.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "query" in result.message


def test_unknown_on_request_exception(backend, _pipeline):
    import requests as req
    with patch("pipewatch.backends.datadog.requests.get", side_effect=req.RequestException("timeout")):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "timeout" in result.message


def test_pipeline_name_in_result(backend, _pipeline):
    with patch("pipewatch.backends.datadog.requests.get") as mock_get:
        mock_get.return_value = _mock_response([[1700000000000, 99.0]])
        result = backend.check_pipeline(_pipeline)
    assert result.pipeline_name == "orders_pipeline"
