"""Tests for the Cassandra backend."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.cassandra import CassandraBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    return CassandraBackend({"host": "cassandra.local", "port": "9042"})


@pytest.fixture()
def _pipeline():
    def _make(query="SELECT count(*) FROM events", threshold=1, keyspace="prod"):
        return SimpleNamespace(
            name="events_pipeline",
            config={"query": query, "threshold": threshold, "keyspace": keyspace},
        )

    return _make


def _make_session_mock(value):
    row = MagicMock()
    row.__getitem__ = lambda self, i: value
    rows = MagicMock()
    rows.one.return_value = row
    session = MagicMock()
    session.execute.return_value = rows
    return session


@patch("pipewatch.backends.cassandra.Cluster", create=True)
def test_healthy_when_count_meets_threshold(mock_cluster_cls, backend, _pipeline):
    session = _make_session_mock(5)
    cluster_inst = MagicMock()
    cluster_inst.connect.return_value = session
    mock_cluster_cls.return_value = cluster_inst

    with patch.dict("sys.modules", {"cassandra": MagicMock(), "cassandra.cluster": MagicMock(), "cassandra.auth": MagicMock()}):
        result = backend.check_pipeline(_pipeline(threshold=1))

    assert result.status == PipelineStatus.HEALTHY


@patch("pipewatch.backends.cassandra.Cluster", create=True)
def test_failed_when_count_below_threshold(mock_cluster_cls, backend, _pipeline):
    session = _make_session_mock(0)
    cluster_inst = MagicMock()
    cluster_inst.connect.return_value = session
    mock_cluster_cls.return_value = cluster_inst

    with patch.dict("sys.modules", {"cassandra": MagicMock(), "cassandra.cluster": MagicMock(), "cassandra.auth": MagicMock()}):
        result = backend.check_pipeline(_pipeline(threshold=1))

    assert result.status == PipelineStatus.FAILED


@patch("pipewatch.backends.cassandra.Cluster", create=True)
def test_unknown_when_no_rows(mock_cluster_cls, backend, _pipeline):
    rows = MagicMock()
    rows.one.return_value = None
    session = MagicMock()
    session.execute.return_value = rows
    cluster_inst = MagicMock()
    cluster_inst.connect.return_value = session
    mock_cluster_cls.return_value = cluster_inst

    with patch.dict("sys.modules", {"cassandra": MagicMock(), "cassandra.cluster": MagicMock(), "cassandra.auth": MagicMock()}):
        result = backend.check_pipeline(_pipeline())

    assert result.status == PipelineStatus.UNKNOWN
    assert "no rows" in result.message.lower()


@patch("pipewatch.backends.cassandra.Cluster", create=True)
def test_unknown_on_connection_error(mock_cluster_cls, backend, _pipeline):
    mock_cluster_cls.side_effect = Exception("connection refused")

    with patch.dict("sys.modules", {"cassandra": MagicMock(), "cassandra.cluster": MagicMock(), "cassandra.auth": MagicMock()}):
        result = backend.check_pipeline(_pipeline())

    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_unknown_when_no_query_configured(backend):
    pipeline = SimpleNamespace(name="no_query", config={"keyspace": "prod"})
    with patch.dict("sys.modules", {"cassandra": MagicMock(), "cassandra.cluster": MagicMock(), "cassandra.auth": MagicMock()}):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "query" in result.message.lower()


def test_result_includes_pipeline_name(backend, _pipeline):
    with patch("pipewatch.backends.cassandra.Cluster", side_effect=Exception("err"), create=True):
        with patch.dict("sys.modules", {"cassandra": MagicMock(), "cassandra.cluster": MagicMock(), "cassandra.auth": MagicMock()}):
            result = backend.check_pipeline(_pipeline())
    assert result.name == "events_pipeline"
