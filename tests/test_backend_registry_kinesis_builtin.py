"""Integration-level test: Kinesis appears in the built-in registry."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pipewatch.backends import get_backend_class


def test_kinesis_in_builtins():
    """Kinesis must be resolvable without explicit registration."""
    import pipewatch.backends.kinesis_register  # noqa: F401
    cls = get_backend_class("kinesis")
    assert cls is not None


def test_kinesis_result_has_pipeline_name():
    import pipewatch.backends.kinesis_register  # noqa: F401
    cls = get_backend_class("kinesis")

    with patch("pipewatch.backends.kinesis.boto3.client"):
        backend = cls({})

    client = MagicMock()
    client.list_shards.return_value = {"Shards": [{"ShardId": "shardId-000000"}]}
    client.get_shard_iterator.return_value = {"ShardIterator": "iter"}
    client.get_records.return_value = {"Records": [], "MillisBehindLatest": 0}
    backend._client = client

    pipeline = SimpleNamespace(
        name="integration-stream",
        extras={"stream_name": "integration-stream"},
    )
    result = backend.check_pipeline(pipeline)
    assert result.pipeline_name == "integration-stream"
