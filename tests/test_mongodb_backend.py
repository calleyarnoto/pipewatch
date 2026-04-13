"""Tests for the MongoDB backend."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.base import PipelineStatus
from pipewatch.backends.mongodb import MongoDBBackend


@pytest.fixture()
def backend() -> MongoDBBackend:
    config = {
        "uri": "mongodb://localhost:27017",
        "database": "etl",
        "collection": "runs",
        "threshold": 1,
        "filter": {},
    }
    with patch.dict("sys.modules", {"pymongo": MagicMock()}):
        return MongoDBBackend(config)


def _pipeline(name: str = "my_pipeline", threshold: int | None = None) -> SimpleNamespace:
    ns = SimpleNamespace(name=name)
    if threshold is not None:
        ns.threshold = threshold
    return ns


def _mock_client(count: int) -> MagicMock:
    mock_collection = MagicMock()
    mock_collection.count_documents.return_value = count
    mock_db = MagicMock()
    mock_db.__getitem__ = MagicMock(return_value=mock_collection)
    mock_client = MagicMock()
    mock_client.__getitem__ = MagicMock(return_value=mock_db)
    return mock_client


def test_healthy_when_count_meets_threshold(backend: MongoDBBackend) -> None:
    with patch("pipewatch.backends.mongodb.pymongo") as mock_pymongo:
        mock_pymongo.MongoClient.return_value = _mock_client(5)
        result = backend.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.HEALTHY
    assert result.pipeline_name == "my_pipeline"


def test_failed_when_count_below_threshold(backend: MongoDBBackend) -> None:
    with patch("pipewatch.backends.mongodb.pymongo") as mock_pymongo:
        mock_pymongo.MongoClient.return_value = _mock_client(0)
        result = backend.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.FAILED
    assert "below threshold" in result.message


def test_custom_threshold_in_pipeline_config(backend: MongoDBBackend) -> None:
    with patch("pipewatch.backends.mongodb.pymongo") as mock_pymongo:
        mock_pymongo.MongoClient.return_value = _mock_client(3)
        result = backend.check_pipeline(_pipeline(threshold=10))
    assert result.status == PipelineStatus.FAILED


def test_unknown_on_exception(backend: MongoDBBackend) -> None:
    with patch("pipewatch.backends.mongodb.pymongo") as mock_pymongo:
        mock_pymongo.MongoClient.side_effect = Exception("connection refused")
        result = backend.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.UNKNOWN
    assert "MongoDB error" in result.message


def test_result_checked_at_is_set(backend: MongoDBBackend) -> None:
    with patch("pipewatch.backends.mongodb.pymongo") as mock_pymongo:
        mock_pymongo.MongoClient.return_value = _mock_client(2)
        result = backend.check_pipeline(_pipeline())
    assert isinstance(result.checked_at, datetime)
    assert result.checked_at.tzinfo is not None
