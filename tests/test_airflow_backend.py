"""Tests for the Airflow backend."""

from __future__ import annotations

import pytest
import responses

from pipewatch.backends.airflow import AirflowBackend
from pipewatch.backends.base import PipelineStatus

BASE_URL = "http://airflow.local"
DAG_ID = "my_etl_dag"
_RUNS_URL = f"{BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns"


@pytest.fixture()
def backend() -> AirflowBackend:
    return AirflowBackend(base_url=BASE_URL, username="admin", password="admin")


def _dag_run_payload(state: str) -> dict:
    return {
        "dag_runs": [
            {
                "dag_id": DAG_ID,
                "dag_run_id": "scheduled__2024-01-01",
                "state": state,
                "execution_date": "2024-01-01T00:00:00+00:00",
            }
        ],
        "total_entries": 1,
    }


@responses.activate
def test_healthy_dag_run(backend: AirflowBackend) -> None:
    responses.add(responses.GET, _RUNS_URL, json=_dag_run_payload("success"), status=200)
    result = backend.check_pipeline(DAG_ID)
    assert result.status == PipelineStatus.HEALTHY
    assert result.pipeline_id == DAG_ID


@responses.activate
def test_failed_dag_run(backend: AirflowBackend) -> None:
    responses.add(responses.GET, _RUNS_URL, json=_dag_run_payload("failed"), status=200)
    result = backend.check_pipeline(DAG_ID)
    assert result.status == PipelineStatus.UNHEALTHY


@responses.activate
def test_running_dag_run_is_unknown(backend: AirflowBackend) -> None:
    responses.add(responses.GET, _RUNS_URL, json=_dag_run_payload("running"), status=200)
    result = backend.check_pipeline(DAG_ID)
    assert result.status == PipelineStatus.UNKNOWN


@responses.activate
def test_no_dag_runs_returns_unknown(backend: AirflowBackend) -> None:
    responses.add(responses.GET, _RUNS_URL, json={"dag_runs": [], "total_entries": 0}, status=200)
    result = backend.check_pipeline(DAG_ID)
    assert result.status == PipelineStatus.UNKNOWN
    assert "No DAG runs found" in result.message


@responses.activate
def test_api_error_returns_unknown(backend: AirflowBackend) -> None:
    responses.add(responses.GET, _RUNS_URL, body=ConnectionError("refused"))
    result = backend.check_pipeline(DAG_ID)
    assert result.status == PipelineStatus.UNKNOWN
    assert result.message != ""


@responses.activate
def test_http_error_returns_unknown(backend: AirflowBackend) -> None:
    responses.add(responses.GET, _RUNS_URL, status=403)
    result = backend.check_pipeline(DAG_ID)
    assert result.status == PipelineStatus.UNKNOWN


def test_backend_name() -> None:
    assert AirflowBackend.name == "airflow"


def test_base_url_trailing_slash_stripped() -> None:
    b = AirflowBackend(base_url="http://airflow.local/")
    assert not b.base_url.endswith("/")
