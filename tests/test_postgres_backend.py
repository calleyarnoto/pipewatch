"""Tests for the PostgreSQL backend."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.base import PipelineStatus
from pipewatch.backends.postgres import PostgresBackend

BASE_CONFIG = {"dsn": "postgresql://user:pass@localhost/db", "threshold": 1}
PIPELINE_CFG = {"query": "SELECT COUNT(*) FROM runs WHERE date = CURRENT_DATE"}


@pytest.fixture()
def backend():
    with patch("pipewatch.backends.postgres.psycopg2", MagicMock()):
        return PostgresBackend(BASE_CONFIG)


def _mock_cursor(return_value):
    """Return a mock psycopg2 cursor context manager yielding *return_value*."""
    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchone.return_value = return_value
    return cursor


def _mock_conn(cursor):
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def test_healthy_when_count_above_threshold(backend):
    cursor = _mock_cursor((5,))
    conn = _mock_conn(cursor)
    with patch("pipewatch.backends.postgres.psycopg2.connect", return_value=conn):
        result = backend.check_pipeline("my_pipeline", PIPELINE_CFG)
    assert result.status == PipelineStatus.HEALTHY
    assert "5" in result.message


def test_failed_when_count_is_zero(backend):
    cursor = _mock_cursor((0,))
    conn = _mock_conn(cursor)
    with patch("pipewatch.backends.postgres.psycopg2.connect", return_value=conn):
        result = backend.check_pipeline("my_pipeline", PIPELINE_CFG)
    assert result.status == PipelineStatus.FAILED


def test_unknown_when_no_rows_returned(backend):
    cursor = _mock_cursor(None)
    conn = _mock_conn(cursor)
    with patch("pipewatch.backends.postgres.psycopg2.connect", return_value=conn):
        result = backend.check_pipeline("my_pipeline", PIPELINE_CFG)
    assert result.status == PipelineStatus.UNKNOWN
    assert "no rows" in result.message.lower()


def test_unknown_when_query_raises(backend):
    with patch(
        "pipewatch.backends.postgres.psycopg2.connect",
        side_effect=Exception("connection refused"),
    ):
        result = backend.check_pipeline("my_pipeline", PIPELINE_CFG)
    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_unknown_when_no_query_configured(backend):
    result = backend.check_pipeline("my_pipeline", {})
    assert result.status == PipelineStatus.UNKNOWN
    assert "No query" in result.message


def test_custom_threshold_respected(backend):
    backend.threshold = 10
    cursor = _mock_cursor((7,))
    conn = _mock_conn(cursor)
    with patch("pipewatch.backends.postgres.psycopg2.connect", return_value=conn):
        result = backend.check_pipeline("my_pipeline", PIPELINE_CFG)
    assert result.status == PipelineStatus.FAILED


def test_result_pipeline_name_set(backend):
    cursor = _mock_cursor((1,))
    conn = _mock_conn(cursor)
    with patch("pipewatch.backends.postgres.psycopg2.connect", return_value=conn):
        result = backend.check_pipeline("orders_etl", PIPELINE_CFG)
    assert result.pipeline_name == "orders_etl"


def test_result_checked_at_is_recent(backend):
    cursor = _mock_cursor((3,))
    conn = _mock_conn(cursor)
    before = datetime.now(timezone.utc)
    with patch("pipewatch.backends.postgres.psycopg2.connect", return_value=conn):
        result = backend.check_pipeline("orders_etl", PIPELINE_CFG)
    after = datetime.now(timezone.utc)
    assert before <= result.checked_at <= after
