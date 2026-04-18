"""Tests for HiveBackend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.hive import HiveBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    return HiveBackend(
        {"host": "hive-host", "port": 10000, "username": "hive", "database": "etl"}
    )


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(
        name="hive_pipeline",
        options={"query": "SELECT COUNT(*) FROM events", "threshold": "5"},
    )


def _make_conn_mock(return_value):
    cursor = MagicMock()
    cursor.fetchone.return_value = (return_value,)
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def test_healthy_when_count_meets_threshold(backend, _pipeline):
    conn_mock = _make_conn_mock(10)
    with patch("pipewatch.backends.hive.hive", create=True) as mock_hive:
        mock_hive.connect.return_value = conn_mock
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
    assert "10" in result.message


def test_failed_when_count_below_threshold(backend, _pipeline):
    conn_mock = _make_conn_mock(2)
    with patch("pipewatch.backends.hive.hive", create=True) as mock_hive:
        mock_hive.connect.return_value = conn_mock
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED
    assert "2" in result.message


def test_unknown_when_no_query(backend):
    pipeline = SimpleNamespace(name="no_query", options={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "No query" in result.message


def test_unknown_on_exception(backend, _pipeline):
    with patch("pipewatch.backends.hive.hive", create=True) as mock_hive:
        mock_hive.connect.side_effect = Exception("connection refused")
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_default_threshold_is_one(backend):
    pipeline = SimpleNamespace(
        name="default_thresh",
        options={"query": "SELECT COUNT(*) FROM t"},
    )
    conn_mock = _make_conn_mock(1)
    with patch("pipewatch.backends.hive.hive", create=True) as mock_hive:
        mock_hive.connect.return_value = conn_mock
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.HEALTHY
