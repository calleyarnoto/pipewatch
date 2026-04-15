"""Tests for the DynamoDB backend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.base import PipelineStatus
from pipewatch.backends.dynamodb import DynamoDBBackend


@pytest.fixture()
def backend() -> DynamoDBBackend:
    with patch("pipewatch.backends.dynamodb.boto3") as mock_boto3:
        mock_boto3.client.return_value = MagicMock()
        instance = DynamoDBBackend({"region": "us-east-1"})
    return instance


def _pipeline(name: str = "test_pipeline", **kwargs) -> SimpleNamespace:
    return SimpleNamespace(name=name, config=kwargs)


def test_healthy_when_count_meets_threshold(backend: DynamoDBBackend) -> None:
    backend._client.scan.return_value = {"Count": 5}
    result = backend.check_pipeline(_pipeline(table="orders", threshold=3))
    assert result.status == PipelineStatus.HEALTHY
    assert "5" in result.message


def test_failed_when_count_below_threshold(backend: DynamoDBBackend) -> None:
    backend._client.scan.return_value = {"Count": 0}
    result = backend.check_pipeline(_pipeline(table="orders", threshold=1))
    assert result.status == PipelineStatus.FAILED
    assert "0" in result.message


def test_default_threshold_is_one(backend: DynamoDBBackend) -> None:
    backend._client.scan.return_value = {"Count": 1}
    result = backend.check_pipeline(_pipeline(table="orders"))
    assert result.status == PipelineStatus.HEALTHY


def test_unknown_when_table_missing(backend: DynamoDBBackend) -> None:
    result = backend.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.UNKNOWN
    assert "table" in result.message.lower()


def test_unknown_on_exception(backend: DynamoDBBackend) -> None:
    backend._client.scan.side_effect = Exception("connection refused")
    result = backend.check_pipeline(_pipeline(table="orders"))
    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_index_passed_to_scan(backend: DynamoDBBackend) -> None:
    backend._client.scan.return_value = {"Count": 10}
    backend.check_pipeline(_pipeline(table="orders", index="status-index"))
    call_kwargs = backend._client.scan.call_args[1]
    assert call_kwargs["IndexName"] == "status-index"


def test_result_name_matches_pipeline(backend: DynamoDBBackend) -> None:
    backend._client.scan.return_value = {"Count": 3}
    result = backend.check_pipeline(_pipeline(name="my_dag", table="events"))
    assert result.name == "my_dag"
