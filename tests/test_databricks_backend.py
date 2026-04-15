"""Tests for the Databricks backend."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from pipewatch.backends.databricks import DatabricksBackend
from pipewatch.backends.base import PipelineStatus

BACKEND_CONFIG = {
    "host": "https://my-workspace.azuredatabricks.net",
    "token": "dapi-test-token",
}


@pytest.fixture()
def backend() -> DatabricksBackend:
    return DatabricksBackend(BACKEND_CONFIG)


@pytest.fixture()
def _pipeline() -> dict:
    return {"name": "my_job", "job_id": "123456"}


def _mock_response(runs: list, status_code: int = 200) -> MagicMock:
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = {"runs": runs}
    mock.raise_for_status = MagicMock()
    return mock


def _run(life_cycle: str, result_state: str) -> dict:
    return {"state": {"life_cycle_state": life_cycle, "result_state": result_state}}


def test_healthy_on_success(backend, _pipeline):
    run = _run("TERMINATED", "SUCCESS")
    with patch.object(backend._session, "get", return_value=_mock_response([run])):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
    assert "SUCCESS" in result.message


def test_failed_on_failed_result_state(backend, _pipeline):
    run = _run("TERMINATED", "FAILED")
    with patch.object(backend._session, "get", return_value=_mock_response([run])):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_failed_on_timedout(backend, _pipeline):
    run = _run("TERMINATED", "TIMEDOUT")
    with patch.object(backend._session, "get", return_value=_mock_response([run])):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_unknown_when_run_not_terminated(backend, _pipeline):
    run = _run("RUNNING", "")
    with patch.object(backend._session, "get", return_value=_mock_response([run])):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "RUNNING" in result.message


def test_unknown_when_no_runs(backend, _pipeline):
    with patch.object(backend._session, "get", return_value=_mock_response([])):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "No runs" in result.message


def test_unknown_on_request_exception(backend, _pipeline):
    with patch.object(
        backend._session, "get", side_effect=requests.ConnectionError("timeout")
    ):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "API error" in result.message


def test_unknown_when_job_id_missing(backend):
    pipeline = {"name": "no_job_id_pipeline"}
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "job_id" in result.message


def test_backend_name():
    assert DatabricksBackend.name == "databricks"
