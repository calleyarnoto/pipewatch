"""Tests for the DuckDB backend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.duckdb import DuckDBBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    return DuckDBBackend({"database": ":memory:"})


@pytest.fixture()
def _pipeline():
    def _make(query="SELECT 5", threshold=1):
        return SimpleNamespace(
            name="test_pipe",
            options={"query": query, "threshold": threshold},
        )
    return _make


def _mock_conn(return_value):
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = [[return_value]]
    conn.__enter__ = lambda s: s
    conn.__exit__ = MagicMock(return_value=False)
    return conn


def test_healthy_when_count_meets_threshold(backend, _pipeline):
    conn = _mock_conn(10)
    with patch("duckdb.connect", return_value=conn):
        result = backend.check_pipeline(_pipeline(threshold=5))
    assert result.status == PipelineStatus.HEALTHY


def test_failed_when_count_below_threshold(backend, _pipeline):
    conn = _mock_conn(0)
    with patch("duckdb.connect", return_value=conn):
        result = backend.check_pipeline(_pipeline(threshold=1))
    assert result.status == PipelineStatus.FAILED


def test_unknown_when_no_query(backend):
    pipeline = SimpleNamespace(name="p", options={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "No query" in result.message


def test_unknown_on_exception(backend, _pipeline):
    with patch("duckdb.connect", side_effect=Exception("boom")):
        result = backend.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.UNKNOWN
    assert "boom" in result.message


def test_unknown_when_null_returned(backend, _pipeline):
    conn = _mock_conn(None)
    with patch("duckdb.connect", return_value=conn):
        result = backend.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.UNKNOWN


def test_default_threshold_is_one(backend, _pipeline):
    pipeline = SimpleNamespace(name="p", options={"query": "SELECT 1"})
    conn = _mock_conn(1)
    with patch("duckdb.connect", return_value=conn):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_pipeline_name_in_result(backend, _pipeline):
    conn = _mock_conn(3)
    with patch("duckdb.connect", return_value=conn):
        result = backend.check_pipeline(_pipeline())
    assert result.pipeline_name == "test_pipe"
