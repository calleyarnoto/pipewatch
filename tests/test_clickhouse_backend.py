from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.clickhouse import ClickHouseBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    with patch("clickhouse_driver.Client"):
        return ClickHouseBackend(
            {"host": "localhost", "port": 9000, "database": "default", "user": "default", "password": ""}
        )


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(name="ch_pipeline", options={"query": "SELECT count() FROM events", "threshold": "1"})


def _make_client_mock(return_value):
    mock_client = MagicMock()
    mock_client.execute.return_value = return_value
    return mock_client


def test_healthy_when_count_meets_threshold(backend, _pipeline):
    with patch("clickhouse_driver.Client", return_value=_make_client_mock([(5,)])):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_failed_when_count_below_threshold(backend, _pipeline):
    _pipeline.options["threshold"] = "10"
    with patch("clickhouse_driver.Client", return_value=_make_client_mock([(0,)])):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_unknown_when_no_query(backend):
    pipeline = SimpleNamespace(name="no_query", options={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "No query" in result.message


def test_unknown_on_exception(backend, _pipeline):
    with patch("clickhouse_driver.Client", side_effect=Exception("connection refused")):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_result_message_includes_value(backend, _pipeline):
    with patch("clickhouse_driver.Client", return_value=_make_client_mock([(42,)])):
        result = backend.check_pipeline(_pipeline)
    assert "42" in result.message


def test_empty_result_treated_as_zero(backend, _pipeline):
    with patch("clickhouse_driver.Client", return_value=_make_client_mock([])):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED
