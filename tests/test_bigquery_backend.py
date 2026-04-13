"""Tests for the BigQuery backend."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.bigquery import BigQueryBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend() -> BigQueryBackend:
    return BigQueryBackend(
        {
            "project": "my-project",
            "dataset": "my_dataset",
            "threshold": 1,
        }
    )


def _bq_client_mock(count: int) -> MagicMock:
    row = SimpleNamespace(cnt=count)
    query_job = MagicMock()
    query_job.result.return_value = iter([row])
    client = MagicMock()
    client.query.return_value = query_job
    return client


def test_healthy_when_count_meets_threshold(backend: BigQueryBackend) -> None:
    with patch("google.cloud.bigquery.Client", return_value=_bq_client_mock(10)):
        result = backend.check_pipeline("events", {"table": "events"})
    assert result.status == PipelineStatus.HEALTHY


def test_failed_when_count_below_threshold(backend: BigQueryBackend) -> None:
    with patch("google.cloud.bigquery.Client", return_value=_bq_client_mock(0)):
        result = backend.check_pipeline("events", {"table": "events"})
    assert result.status == PipelineStatus.FAILED


def test_unknown_on_exception(backend: BigQueryBackend) -> None:
    with patch("google.cloud.bigquery.Client", side_effect=Exception("auth error")):
        result = backend.check_pipeline("events", {"table": "events"})
    assert result.status == PipelineStatus.UNKNOWN
    assert "BigQuery error" in result.message


def test_pipeline_name_preserved(backend: BigQueryBackend) -> None:
    with patch("google.cloud.bigquery.Client", return_value=_bq_client_mock(5)):
        result = backend.check_pipeline("my_pipeline", {"table": "t"})
    assert result.pipeline_name == "my_pipeline"


def test_custom_threshold_per_pipeline(backend: BigQueryBackend) -> None:
    with patch("google.cloud.bigquery.Client", return_value=_bq_client_mock(3)):
        result = backend.check_pipeline("events", {"table": "events", "threshold": "10"})
    assert result.status == PipelineStatus.FAILED


def test_where_clause_included_in_query(backend: BigQueryBackend) -> None:
    client_mock = _bq_client_mock(1)
    with patch("google.cloud.bigquery.Client", return_value=client_mock):
        backend.check_pipeline("events", {"table": "events", "where": "dt='2024-01-01'"})
    query_str: str = client_mock.query.call_args[0][0]
    assert "WHERE" in query_str
    assert "dt='2024-01-01'" in query_str
