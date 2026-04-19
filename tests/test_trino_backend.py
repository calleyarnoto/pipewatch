"""Tests for TrinoBackend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.trino import TrinoBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    return TrinoBackend(
        {"host": "trino.local", "port": 8080, "user": "pw", "catalog": "hive", "schema": "etl"}
    )


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(name="orders_pipeline", extra={"query": "SELECT COUNT(*) FROM orders", "threshold": 1})


@pytest.fixture()
def _make_conn_mock():
    def _make(return_value):
        cursor = MagicMock()
        cursor.fetchone.return_value = (return_value,)
        conn = MagicMock()
        conn.cursor.return_value = cursor
        return conn
    return _make


def test_healthy_when_count_meets_threshold(backend, _pipeline, _make_conn_mock):
    conn = _make_conn_mock(5)
    with patch("pipewatch.backends.trino.trino") as mock_trino:
        mock_trino.dbapi.connect.return_value = conn
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
    assert "5" in result.message


def test_failed_when_count_below_threshold(backend, _pipeline, _make_conn_mock):
    conn = _make_conn_mock(0)
    with patch("pipewatch.backends.trino.trino") as mock_trino:
        mock_trino.dbapi.connect.return_value = conn
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_custom_threshold(backend, _make_conn_mock):
    pipeline = SimpleNamespace(name="p", extra={"query": "SELECT 3", "threshold": 10})
    conn = _make_conn_mock(3)
    with patch("pipewatch.backends.trino.trino") as mock_trino:
        mock_trino.dbapi.connect.return_value = conn
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.FAILED
    assert "10" in result.message


def test_unknown_when_no_query(backend):
    pipeline = SimpleNamespace(name="p", extra={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_unknown_on_exception(backend, _pipeline):
    with patch("pipewatch.backends.trino.trino") as mock_trino:
        mock_trino.dbapi.connect.side_effect = RuntimeError("connection refused")
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_none_row_treated_as_zero(backend, _make_conn_mock):
    cursor = MagicMock()
    cursor.fetchone.return_value = None
    conn = MagicMock()
    conn.cursor.return_value = cursor
    with patch("pipewatch.backends.trino.trino") as mock_trino:
        mock_trino.dbapi.connect.return_value = conn
        result = backend.check_pipeline(
            SimpleNamespace(name="p", extra={"query": "SELECT COUNT(*) FROM t", "threshold": 1})
        )
    assert result.status == PipelineStatus.FAILED
