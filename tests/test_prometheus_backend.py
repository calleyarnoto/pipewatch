"""Tests for the Prometheus backend."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from pipewatch.backends.prometheus import PrometheusBackend
from pipewatch.backends.base import PipelineStatus

BASE_OPTIONS = {
    "base_url": "http://prometheus.local:9090",
    "query_template": "up{job='{pipeline}'}",
    "timeout": 5,
}


@pytest.fixture()
def backend() -> PrometheusBackend:
    return PrometheusBackend(BASE_OPTIONS)


def _mock_response(results: list) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {"data": {"result": results}}
    resp.raise_for_status.return_value = None
    return resp


@patch("pipewatch.backends.prometheus.requests.get")
def test_healthy_when_metric_nonzero(mock_get, backend):
    mock_get.return_value = _mock_response([[{}, [0, "1"]]])
    result = backend.check_pipeline("my_pipeline")
    assert result.status == PipelineStatus.HEALTHY
    assert result.pipeline_name == "my_pipeline"


@patch("pipewatch.backends.prometheus.requests.get")
def test_failed_when_metric_zero(mock_get, backend):
    mock_get.return_value = _mock_response([[{}, [0, "0"]]])
    result = backend.check_pipeline("my_pipeline")
    assert result.status == PipelineStatus.FAILED


@patch("pipewatch.backends.prometheus.requests.get")
def test_unknown_when_no_results(mock_get, backend):
    mock_get.return_value = _mock_response([])
    result = backend.check_pipeline("my_pipeline")
    assert result.status == PipelineStatus.UNKNOWN
    assert "No data" in result.message


@patch("pipewatch.backends.prometheus.requests.get")
def test_unknown_on_request_exception(mock_get, backend):
    mock_get.side_effect = requests.ConnectionError("refused")
    result = backend.check_pipeline("my_pipeline")
    assert result.status == PipelineStatus.UNKNOWN
    assert "refused" in result.message


@patch("pipewatch.backends.prometheus.requests.get")
def test_unknown_on_http_error(mock_get, backend):
    """A non-2xx HTTP response should result in UNKNOWN status."""
    resp = MagicMock()
    resp.raise_for_status.side_effect = requests.HTTPError("503 Service Unavailable")
    mock_get.return_value = resp
    result = backend.check_pipeline("my_pipeline")
    assert result.status == PipelineStatus.UNKNOWN
    assert "503" in result.message


@patch("pipewatch.backends.prometheus.requests.get")
def test_query_uses_pipeline_name(mock_get, backend):
    mock_get.return_value = _mock_response([[{}, [0, "1"]]])
    backend.check_pipeline("sales_etl")
    _, kwargs = mock_get.call_args
    assert "sales_etl" in kwargs["params"]["query"]


@patch("pipewatch.backends.prometheus.requests.get")
def test_timeout_passed_to_request(mock_get, backend):
    mock_get.return_value = _mock_response([[{}, [0, "1"]]])
    backend.check_pipeline("sales_etl")
    _, kwargs = mock_get.call_args
    assert kwargs["timeout"] == 5


def test_default_timeout():
    opts = {**BASE_OPTIONS}
    opts.pop("timeout")
    b = PrometheusBackend(opts)
    assert b._timeout == 10
