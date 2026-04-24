"""Tests for the dbt Cloud backend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.dbt import DBTBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    return DBTBackend({"account_id": "123", "api_token": "secret"})


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(name="my_dbt_job", options={"job_id": "456"})


def _mock_response(status_humanized: str, run_id: int = 1):
    """Build a mock requests.Response for a dbt Cloud runs list endpoint."""
    resp = MagicMock()
    resp.json.return_value = {"data": [{"id": run_id, "status_humanized": status_humanized}]}
    resp.raise_for_status.return_value = None
    return resp


def test_healthy_on_success(backend, _pipeline):
    with patch("pipewatch.backends.dbt.requests.get", return_value=_mock_response("Success")):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_failed_on_error(backend, _pipeline):
    with patch("pipewatch.backends.dbt.requests.get", return_value=_mock_response("Error")):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_failed_on_cancelled(backend, _pipeline):
    with patch("pipewatch.backends.dbt.requests.get", return_value=_mock_response("Cancelled")):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_unknown_on_running(backend, _pipeline):
    with patch("pipewatch.backends.dbt.requests.get", return_value=_mock_response("Running")):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_unknown_when_no_runs(backend, _pipeline):
    resp = MagicMock()
    resp.json.return_value = {"data": []}
    resp.raise_for_status.return_value = None
    with patch("pipewatch.backends.dbt.requests.get", return_value=resp):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "No runs" in result.message


def test_unknown_on_request_exception(backend, _pipeline):
    import requests as req
    with patch("pipewatch.backends.dbt.requests.get", side_effect=req.RequestException("timeout")):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "timeout" in result.message


def test_unknown_when_missing_job_id(backend):
    pipeline = SimpleNamespace(name="no_job", options={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "job_id" in result.message


@pytest.mark.parametrize("status_humanized,expected", [
    ("Success", PipelineStatus.HEALTHY),
    ("Error", PipelineStatus.FAILED),
    ("Cancelled", PipelineStatus.FAILED),
    ("Running", PipelineStatus.UNKNOWN),
    ("Queued", PipelineStatus.UNKNOWN),
])
def test_status_mapping(backend, _pipeline, status_humanized, expected):
    """Verify that each dbt run status maps to the correct PipelineStatus."""
    with patch("pipewatch.backends.dbt.requests.get", return_value=_mock_response(status_humanized)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == expected
