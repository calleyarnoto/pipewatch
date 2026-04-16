"""Tests for SplunkBackend."""
from __future__ import annotations

import json
import pytest
from unittest.mock import MagicMock, patch

from pipewatch.backends.splunk import SplunkBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    return SplunkBackend(
        {"base_url": "https://splunk.example.com:8089", "token": "test-token"}
    )


@pytest.fixture()
def _pipeline():
    p = MagicMock()
    p.name = "etl_pipeline"
    p.extras = {"query": "index=etl | stats count", "field": "count", "threshold": "5"}
    return p


def _mock_response(data: dict, status_code: int = 200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = json.dumps({"result": data})
    resp.raise_for_status = MagicMock()
    return resp


def test_healthy_when_count_meets_threshold(backend, _pipeline):
    with patch("pipewatch.backends.splunk.requests.get") as mock_get:
        mock_get.return_value = _mock_response({"count": "10"})
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_failed_when_count_below_threshold(backend, _pipeline):
    with patch("pipewatch.backends.splunk.requests.get") as mock_get:
        mock_get.return_value = _mock_response({"count": "2"})
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_unknown_on_request_exception(backend, _pipeline):
    import requests as req
    with patch("pipewatch.backends.splunk.requests.get", side_effect=req.RequestException("timeout")):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "timeout" in result.message


def test_unknown_when_no_query(backend):
    p = MagicMock()
    p.name = "no_query"
    p.extras = {}
    result = backend.check_pipeline(p)
    assert result.status == PipelineStatus.UNKNOWN
    assert "query" in result.message.lower()


def test_unknown_when_field_missing(backend, _pipeline):
    with patch("pipewatch.backends.splunk.requests.get") as mock_get:
        mock_get.return_value = _mock_response({"other_field": "99"})
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_message_contains_value_and_threshold(backend, _pipeline):
    with patch("pipewatch.backends.splunk.requests.get") as mock_get:
        mock_get.return_value = _mock_response({"count": "7"})
        result = backend.check_pipeline(_pipeline)
    assert "7" in result.message
    assert "5" in result.message
