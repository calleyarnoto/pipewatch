"""Tests for the CockroachDB backend."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.cockroachdb import CockroachDBBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    return CockroachDBBackend(
        {
            "host": "crdb-host",
            "port": 26257,
            "database": "testdb",
            "user": "root",
            "password": "",
            "sslmode": "require",
        }
    )


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(
        name="crdb_pipeline",
        config={"query": "SELECT COUNT(*) FROM events", "threshold": 1},
    )


@pytest.fixture()
def _make_conn_mock():
    """Factory that returns a patched psycopg2 connection yielding a given value."""

    def _factory(row_value):
        cursor_mock = MagicMock()
        cursor_mock.__enter__ = lambda s: s
        cursor_mock.__exit__ = MagicMock(return_value=False)
        cursor_mock.fetchone.return_value = (row_value,)

        conn_mock = MagicMock()
        conn_mock.cursor.return_value = cursor_mock
        return conn_mock

    return _factory


def test_healthy_when_count_meets_threshold(backend, _pipeline, _make_conn_mock):
    with patch.object(backend, "_connect", return_value=_make_conn_mock(5)):
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.HEALTHY
    assert result.pipeline_name == "crdb_pipeline"


def test_failed_when_count_below_threshold(backend, _pipeline, _make_conn_mock):
    with patch.object(backend, "_connect", return_value=_make_conn_mock(0)):
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.FAILED


def test_custom_threshold_respected(backend, _make_conn_mock):
    pipeline = SimpleNamespace(
        name="crdb_pipeline",
        config={"query": "SELECT COUNT(*) FROM events", "threshold": 10},
    )
    with patch.object(backend, "_connect", return_value=_make_conn_mock(9)):
        result = backend.check_pipeline(pipeline)

    assert result.status == PipelineStatus.FAILED
    assert "9" in result.message
    assert "10" in result.message


def test_unknown_on_missing_query(backend):
    pipeline = SimpleNamespace(name="crdb_pipeline", config={})
    result = backend.check_pipeline(pipeline)

    assert result.status == PipelineStatus.UNKNOWN
    assert "No query" in result.message


def test_unknown_on_connection_error(backend, _pipeline):
    with patch.object(backend, "_connect", side_effect=Exception("connection refused")):
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_result_pipeline_name_set(backend, _pipeline, _make_conn_mock):
    with patch.object(backend, "_connect", return_value=_make_conn_mock(3)):
        result = backend.check_pipeline(_pipeline)

    assert result.pipeline_name == _pipeline.name
