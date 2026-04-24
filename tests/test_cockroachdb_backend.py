"""Tests for the CockroachDB backend."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.cockroachdb import CockroachDBBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend() -> CockroachDBBackend:
    return CockroachDBBackend(
        {
            "host": "crdb-host",
            "port": 26257,
            "database": "mydb",
            "user": "root",
            "password": "",
            "sslmode": "disable",
        }
    )


@pytest.fixture()
def _pipeline() -> SimpleNamespace:
    return SimpleNamespace(
        name="crdb_pipeline",
        options={"query": "SELECT COUNT(*) FROM events", "threshold": "1"},
    )


def _make_conn_mock(return_value: object) -> MagicMock:
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchone.return_value = (return_value,)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def test_healthy_when_count_meets_threshold(backend, _pipeline):
    conn = _make_conn_mock(5)
    with patch.object(backend, "_connect", return_value=conn):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
    assert result.pipeline_name == "crdb_pipeline"


def test_failed_when_count_below_threshold(backend, _pipeline):
    conn = _make_conn_mock(0)
    with patch.object(backend, "_connect", return_value=conn):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_custom_threshold(backend):
    pipeline = SimpleNamespace(
        name="crdb_custom",
        options={"query": "SELECT COUNT(*) FROM jobs", "threshold": "10"},
    )
    conn = _make_conn_mock(9)
    with patch.object(backend, "_connect", return_value=conn):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.FAILED


def test_unknown_on_exception(backend, _pipeline):
    with patch.object(backend, "_connect", side_effect=Exception("connection refused")):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_unknown_when_no_rows(backend, _pipeline):
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchone.return_value = None
    conn = MagicMock()
    conn.cursor.return_value = cursor
    with patch.object(backend, "_connect", return_value=conn):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_dsn_used_when_provided():
    backend = CockroachDBBackend({"dsn": "postgresql://root@crdb:26257/mydb"})
    pipeline = SimpleNamespace(
        name="dsn_pipeline",
        options={"query": "SELECT 1", "threshold": "1"},
    )
    conn = _make_conn_mock(1)
    with patch("psycopg2.connect", return_value=conn) as mock_connect:
        backend.check_pipeline(pipeline)
    mock_connect.assert_called_once_with("postgresql://root@crdb:26257/mydb")
