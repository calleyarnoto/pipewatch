"""Tests for the TimescaleDB backend."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.timescaledb import TimescaleDBBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend() -> TimescaleDBBackend:
    return TimescaleDBBackend(
        {
            "host": "localhost",
            "port": 5432,
            "dbname": "warehouse",
            "user": "pipewatch",
            "password": "secret",
        }
    )


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(
        name="ts_pipeline",
        params={"query": "SELECT COUNT(*) FROM events WHERE ts > NOW() - INTERVAL '1 hour'", "threshold": 10},
    )


def _make_conn_mock(return_value):
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchone.return_value = (return_value,)

    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def test_healthy_when_count_meets_threshold(backend, _pipeline):
    conn = _make_conn_mock(42)
    with patch("psycopg2.connect", return_value=conn):
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.HEALTHY
    assert "42" in result.message


def test_failed_when_count_below_threshold(backend, _pipeline):
    conn = _make_conn_mock(3)
    with patch("psycopg2.connect", return_value=conn):
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.FAILED
    assert "3" in result.message


def test_unknown_when_query_returns_none(backend):
    pipeline = SimpleNamespace(name="ts_pipeline", params={"query": "SELECT 1"})
    conn = _make_conn_mock(None)
    with patch("psycopg2.connect", return_value=conn):
        result = backend.check_pipeline(pipeline)

    assert result.status == PipelineStatus.UNKNOWN


def test_unknown_when_no_query_specified(backend):
    pipeline = SimpleNamespace(name="ts_pipeline", params={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "No query" in result.message


def test_unknown_on_exception(backend, _pipeline):
    with patch("psycopg2.connect", side_effect=Exception("connection refused")):
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_default_threshold_is_one(backend):
    pipeline = SimpleNamespace(name="ts_pipeline", params={"query": "SELECT 1"})
    conn = _make_conn_mock(1)
    with patch("psycopg2.connect", return_value=conn):
        result = backend.check_pipeline(pipeline)

    assert result.status == PipelineStatus.HEALTHY
