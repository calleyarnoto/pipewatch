from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.databricks import DatabricksBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture
def backend():
    return DatabricksBackend({"host": "https://adb-123.azuredatabricks.net", "token": "dapi-secret"})


def _pipeline(name="my_job", params=None):
    p = MagicMock()
    p.name = name
    p.params = params or {"job_id": "42"}
    return p


def _mock_response(runs):
    resp = MagicMock()
    resp.json.return_value = {"runs": runs}
    resp.raise_for_status.return_value = None
    return resp


def _run_state(life_cycle, result_state=""):
    return {"state": {"life_cycle_state": life_cycle, "result_state": result_state}}


@patch("pipewatch.backends.databricks.requests.get")
def test_healthy_on_success(mock_get, backend):
    mock_get.return_value = _mock_response([_run_state("TERMINATED", "SUCCESS")])
    result = backend.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.HEALTHY


@patch("pipewatch.backends.databricks.requests.get")
def test_failed_on_failed_result(mock_get, backend):
    mock_get.return_value = _mock_response([_run_state("TERMINATED", "FAILED")])
    result = backend.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.FAILED


@patch("pipewatch.backends.databricks.requests.get")
def test_unknown_when_running(mock_get, backend):
    mock_get.return_value = _mock_response([_run_state("RUNNING")])
    result = backend.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.UNKNOWN


@patch("pipewatch.backends.databricks.requests.get")
def test_unknown_when_no_runs(mock_get, backend):
    mock_get.return_value = _mock_response([])
    result = backend.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.UNKNOWN


@patch("pipewatch.backends.databricks.requests.get")
def test_unknown_on_request_exception(mock_get, backend):
    import requests
    mock_get.side_effect = requests.RequestException("timeout")
    result = backend.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.UNKNOWN
    assert "timeout" in result.message


def test_missing_job_id_returns_unknown(backend):
    result = backend.check_pipeline(_pipeline(params={}))
    assert result.status == PipelineStatus.UNKNOWN
    assert "job_id" in result.message


@patch("pipewatch.backends.databricks.requests.get")
def test_result_pipeline_name_preserved(mock_get, backend):
    mock_get.return_value = _mock_response([_run_state("TERMINATED", "SUCCESS")])
    result = backend.check_pipeline(_pipeline(name="etl_nightly"))
    assert result.pipeline_name == "etl_nightly"
