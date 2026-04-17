"""Tests for InfluxDBBackend."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.influxdb import InfluxDBBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    return InfluxDBBackend({"url": "http://influx:8086", "token": "tok", "org": "myorg"})


@pytest.fixture()
def _pipeline():
    p = MagicMock()
    p.name = "my_pipeline"
    p.options = {"query": "from(bucket:\"b\") |> range(start:-1h) |> count()"}
    return p


def _mock_response(text, status=200):
    r = MagicMock()
    r.status_code = status
    r.text = text
    r.raise_for_status = MagicMock()
    return r


CSV_HEADER = "#group,#datatype\r\n,result,table,_value\r\n"


def test_healthy_when_count_meets_threshold(backend, _pipeline):
    csv = CSV_HEADER + ",_result,0,5\r\n"
    with patch("requests.post", return_value=_mock_response(csv)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_failed_when_count_below_threshold(backend, _pipeline):
    _pipeline.options["threshold"] = 10
    csv = CSV_HEADER + ",_result,0,3\r\n"
    with patch("requests.post", return_value=_mock_response(csv)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_unknown_when_no_results(backend, _pipeline):
    csv = CSV_HEADER  # no data rows
    with patch("requests.post", return_value=_mock_response(csv)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_unknown_on_request_exception(backend, _pipeline):
    with patch("requests.post", side_effect=Exception("timeout")):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "timeout" in result.message


def test_unknown_when_no_query(backend):
    p = MagicMock()
    p.name = "p"
    p.options = {}
    result = backend.check_pipeline(p)
    assert result.status == PipelineStatus.UNKNOWN
    assert "query" in result.message.lower()


def test_custom_threshold_respected(backend, _pipeline):
    _pipeline.options["threshold"] = 2
    csv = CSV_HEADER + ",_result,0,2\r\n"
    with patch("requests.post", return_value=_mock_response(csv)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
