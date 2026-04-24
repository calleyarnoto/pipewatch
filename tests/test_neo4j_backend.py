"""Tests for the Neo4j backend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.neo4j import Neo4jBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend() -> Neo4jBackend:
    return Neo4jBackend(uri="bolt://localhost:7687", username="neo4j", password="secret")


def _pipeline(extras: dict | None = None):
    return SimpleNamespace(name="graph-pipe", extras=extras or {})


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_driver_mock(return_value):
    record = MagicMock()
    record.__getitem__ = MagicMock(side_effect=lambda i: return_value)

    session = MagicMock()
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)
    session.run.return_value.single.return_value = record

    driver = MagicMock()
    driver.session.return_value = session
    return driver


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_healthy_when_count_meets_threshold(backend):
    driver = _make_driver_mock(5)
    with patch("pipewatch.backends.neo4j.GraphDatabase") as gdb:
        gdb.driver.return_value = driver
        result = backend.check_pipeline(
            _pipeline({"query": "MATCH (n) RETURN count(n)", "threshold": 3})
        )
    assert result.status == PipelineStatus.HEALTHY
    assert result.pipeline_name == "graph-pipe"


def test_failed_when_count_below_threshold(backend):
    driver = _make_driver_mock(0)
    with patch("pipewatch.backends.neo4j.GraphDatabase") as gdb:
        gdb.driver.return_value = driver
        result = backend.check_pipeline(
            _pipeline({"query": "MATCH (n) RETURN count(n)", "threshold": 1})
        )
    assert result.status == PipelineStatus.FAILED


def test_default_threshold_is_one(backend):
    driver = _make_driver_mock(1)
    with patch("pipewatch.backends.neo4j.GraphDatabase") as gdb:
        gdb.driver.return_value = driver
        result = backend.check_pipeline(
            _pipeline({"query": "MATCH (n) RETURN count(n)"})
        )
    assert result.status == PipelineStatus.HEALTHY


def test_missing_query_returns_unknown(backend):
    result = backend.check_pipeline(_pipeline({}))
    assert result.status == PipelineStatus.UNKNOWN
    assert "query" in result.message


def test_unknown_on_driver_exception(backend):
    with patch("pipewatch.backends.neo4j.GraphDatabase") as gdb:
        gdb.driver.side_effect = RuntimeError("connection refused")
        result = backend.check_pipeline(
            _pipeline({"query": "MATCH (n) RETURN count(n)"})
        )
    assert result.status == PipelineStatus.UNKNOWN
    assert "Neo4j error" in result.message


def test_unknown_when_no_rows(backend):
    session = MagicMock()
    session.__enter__ = MagicMock(return_value=session)
    session.__exit__ = MagicMock(return_value=False)
    session.run.return_value.single.return_value = None
    driver = MagicMock()
    driver.session.return_value = session
    with patch("pipewatch.backends.neo4j.GraphDatabase") as gdb:
        gdb.driver.return_value = driver
        result = backend.check_pipeline(
            _pipeline({"query": "MATCH (n:Missing) RETURN count(n)"})
        )
    assert result.status == PipelineStatus.UNKNOWN
    assert "no rows" in result.message


def test_unknown_when_neo4j_not_installed(backend):
    with patch.dict("sys.modules", {"neo4j": None}):
        result = backend.check_pipeline(
            _pipeline({"query": "MATCH (n) RETURN count(n)"})
        )
    assert result.status == PipelineStatus.UNKNOWN
    assert "not installed" in result.message
