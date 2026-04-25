"""Tests for the Kinesis backend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from botocore.exceptions import ClientError

from pipewatch.backends.kinesis import KinesisBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    with patch("pipewatch.backends.kinesis.boto3.client"):
        return KinesisBackend({})


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(
        name="my-stream",
        extras={"stream_name": "my-stream", "max_lag_ms": 60_000},
    )


def _make_client_mock(backend, lag_ms: int, num_shards: int = 1):
    """Wire up the mocked boto3 client on *backend*."""
    client = MagicMock()
    shards = [{"ShardId": f"shardId-{i:06d}"} for i in range(num_shards)]
    client.list_shards.return_value = {"Shards": shards}
    client.get_shard_iterator.return_value = {"ShardIterator": "fake-iter"}
    client.get_records.return_value = {"Records": [], "MillisBehindLatest": lag_ms}
    backend._client = client
    return client


def test_healthy_when_lag_within_threshold(backend, _pipeline):
    _make_client_mock(backend, lag_ms=1_000)
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
    assert result.pipeline_name == "my-stream"


def test_failed_when_lag_exceeds_threshold(backend, _pipeline):
    _make_client_mock(backend, lag_ms=120_000)
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED
    assert "120000" in result.message


def test_custom_threshold_respected(backend):
    pipeline = SimpleNamespace(
        name="fast-stream",
        extras={"stream_name": "fast-stream", "max_lag_ms": 5_000},
    )
    _make_client_mock(backend, lag_ms=6_000)
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.FAILED


def test_unknown_when_no_shards(backend, _pipeline):
    client = MagicMock()
    client.list_shards.return_value = {"Shards": []}
    backend._client = client
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "No shards" in result.message


def test_unknown_on_client_error(backend, _pipeline):
    client = MagicMock()
    client.list_shards.side_effect = ClientError(
        {"Error": {"Code": "ResourceNotFoundException", "Message": "not found"}},
        "ListShards",
    )
    backend._client = client
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_max_lag_across_multiple_shards(backend, _pipeline):
    """The worst shard drives the result."""
    client = MagicMock()
    shards = [{"ShardId": "shardId-000000"}, {"ShardId": "shardId-000001"}]
    client.list_shards.return_value = {"Shards": shards}
    client.get_shard_iterator.return_value = {"ShardIterator": "iter"}
    # First shard fine, second shard over threshold
    client.get_records.side_effect = [
        {"Records": [], "MillisBehindLatest": 1_000},
        {"Records": [], "MillisBehindLatest": 90_000},
    ]
    backend._client = client
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED
    assert "90000" in result.message
