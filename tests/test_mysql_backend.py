"""Tests for the MySQL backend."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.mysql import MySQLBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend() -> MySQLBackend:
    return MySQLBackend(
        {
            "host": "localhost",
            "port": 3306,
            "user": "root",
            "password": "secret",
            "database": "warehouse",
            "threshold": 1,
        }
    )


def _mock_conn(count: int) -> MagicMock:
    cursor = MagicMock()
    cursor.fetchone.return_value = (count,)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def test_healthy_when_count_meets_threshold(backend: MySQLBackend) -> None:
    with patch("mysql.connector.connect", return_value=_mock_conn(5)):
        result = backend.check_pipeline("orders", {"table": "orders"})
    assert result.status == PipelineStatus.HEALTHY
    assert "5" in result.message


def test_failed_when_count_below_threshold(backend: MySQLBackend) -> None:
    with patch("mysql.connector.connect", return_value=_mock_conn(0)):
        result = backend.check_pipeline("orders", {"table": "orders"})
    assert result.status == PipelineStatus.FAILED
    assert "0" in result.message


def test_custom_threshold_in_pipeline_config(backend: MySQLBackend) -> None:
    with patch("mysql.connector.connect", return_value=_mock_conn(3)):
        result = backend.check_pipeline("orders", {"table": "orders", "threshold": "10"})
    assert result.status == PipelineStatus.FAILED


def test_where_clause_appended(backend: MySQLBackend) -> None:
    conn = _mock_conn(2)
    with patch("mysql.connector.connect", return_value=conn):
        backend.check_pipeline("orders", {"table": "orders", "where": "status='done'"})
    cursor = conn.cursor.return_value
    executed_query: str = cursor.execute.call_args[0][0]
    assert "WHERE" in executed_query
    assert "status='done'" in executed_query


def test_unknown_on_connection_error(backend: MySQLBackend) -> None:
    with patch("mysql.connector.connect", side_effect=Exception("connection refused")):
        result = backend.check_pipeline("orders", {"table": "orders"})
    assert result.status == PipelineStatus.UNKNOWN
    assert "MySQL error" in result.message


def test_pipeline_name_in_result(backend: MySQLBackend) -> None:
    with patch("mysql.connector.connect", return_value=_mock_conn(1)):
        result = backend.check_pipeline("my_pipeline", {"table": "my_table"})
    assert result.pipeline_name == "my_pipeline"


def test_table_defaults_to_pipeline_name(backend: MySQLBackend) -> None:
    conn = _mock_conn(1)
    with patch("mysql.connector.connect", return_value=conn):
        backend.check_pipeline("my_table", {})
    cursor = conn.cursor.return_value
    executed_query: str = cursor.execute.call_args[0][0]
    assert "my_table" in executed_query
