"""Tests for the Snowflake backend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.snowflake import SnowflakeBackend
from pipewatch.backends.base import PipelineStatus


_CONFIG = {
    "account": "myorg-myaccount",
    "user": "testuser",
    "password": "secret",
    "database": "ANALYTICS",
    "schema": "PUBLIC",
    "warehouse": "COMPUTE_WH",
    "role": "SYSADMIN",
}

_PIPELINE = {
    "name": "daily_orders",
    "query": "SELECT COUNT(*) FROM orders WHERE created_at >= CURRENT_DATE",
    "threshold": 10,
}


@pytest.fixture()
def backend() -> SnowflakeBackend:
    return SnowflakeBackend(_CONFIG)


def _mock_conn(row: tuple) -> MagicMock:
    cur = MagicMock()
    cur.fetchone.return_value = row
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn


def test_healthy_when_count_meets_threshold(backend: SnowflakeBackend) -> None:
    with patch("snowflake.connector.connect", return_value=_mock_conn((42,))):
        result = backend.check_pipeline(_PIPELINE)
    assert result.status == PipelineStatus.HEALTHY
    assert "42" in result.message


def test_failed_when_count_below_threshold(backend: SnowflakeBackend) -> None:
    with patch("snowflake.connector.connect", return_value=_mock_conn((3,))):
        result = backend.check_pipeline(_PIPELINE)
    assert result.status == PipelineStatus.FAILED
    assert "3" in result.message


def test_unknown_on_exception(backend: SnowflakeBackend) -> None:
    with patch(
        "snowflake.connector.connect", side_effect=Exception("network error")
    ):
        result = backend.check_pipeline(_PIPELINE)
    assert result.status == PipelineStatus.UNKNOWN
    assert "network error" in result.message


def test_default_threshold_is_one(backend: SnowflakeBackend) -> None:
    pipeline = {**_PIPELINE}
    pipeline.pop("threshold", None)
    with patch("snowflake.connector.connect", return_value=_mock_conn((1,))):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_failed_when_fetchone_returns_none(backend: SnowflakeBackend) -> None:
    with patch("snowflake.connector.connect", return_value=_mock_conn(None)):
        result = backend.check_pipeline(_PIPELINE)
    assert result.status == PipelineStatus.FAILED


def test_result_name_matches_pipeline(backend: SnowflakeBackend) -> None:
    with patch("snowflake.connector.connect", return_value=_mock_conn((20,))):
        result = backend.check_pipeline(_PIPELINE)
    assert result.name == "daily_orders"
