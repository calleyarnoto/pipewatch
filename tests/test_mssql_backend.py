"""Tests for the MSSQL backend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.mssql import MSSQLBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    return MSSQLBackend(
        {
            "host": "db.example.com",
            "port": 1433,
            "database": "warehouse",
            "user": "pipewatch",
            "password": "secret",
        }
    )


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(
        name="mssql_pipeline",
        params={"query": "SELECT COUNT(*) FROM orders", "threshold": 1},
    )


def _make_conn_mock(return_value):
    cursor = MagicMock()
    cursor.fetchone.return_value = (return_value,)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    return conn


def test_healthy_when_count_meets_threshold(backend, _pipeline):
    conn = _make_conn_mock(42)
    with patch("pipewatch.backends.mssql.pyodbc", create=True) as mock_pyodbc:
        mock_pyodbc.connect.return_value = conn
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
    assert "42" in result.message


def test_failed_when_count_below_threshold(backend, _pipeline):
    conn = _make_conn_mock(0)
    with patch("pipewatch.backends.mssql.pyodbc", create=True) as mock_pyodbc:
        mock_pyodbc.connect.return_value = conn
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED
    assert "0" in result.message


def test_custom_threshold(backend):
    pipeline = SimpleNamespace(
        name="mssql_pipeline",
        params={"query": "SELECT COUNT(*) FROM events", "threshold": 100},
    )
    conn = _make_conn_mock(99)
    with patch("pipewatch.backends.mssql.pyodbc", create=True) as mock_pyodbc:
        mock_pyodbc.connect.return_value = conn
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.FAILED


def test_unknown_when_no_query(backend):
    pipeline = SimpleNamespace(name="mssql_pipeline", params={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "query" in result.message.lower()


def test_unknown_on_connection_error(backend, _pipeline):
    with patch("pipewatch.backends.mssql.pyodbc", create=True) as mock_pyodbc:
        mock_pyodbc.connect.side_effect = Exception("connection refused")
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_unknown_when_query_returns_no_rows(backend, _pipeline):
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    conn = MagicMock()
    conn.cursor.return_value = cursor
    with patch("pipewatch.backends.mssql.pyodbc", create=True) as mock_pyodbc:
        mock_pyodbc.connect.return_value = conn
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "no rows" in result.message.lower()


def test_result_carries_pipeline_name(backend, _pipeline):
    conn = _make_conn_mock(5)
    with patch("pipewatch.backends.mssql.pyodbc", create=True) as mock_pyodbc:
        mock_pyodbc.connect.return_value = conn
        result = backend.check_pipeline(_pipeline)
    assert result.pipeline_name == "mssql_pipeline"
