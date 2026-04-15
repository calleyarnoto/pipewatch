"""Tests for the Kafka backend."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.kafka import KafkaBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend() -> KafkaBackend:
    return KafkaBackend({"bootstrap_servers": "broker:9092"})


def _pipeline(name: str = "orders", **extra):
    return SimpleNamespace(name=name, extra=extra)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _make_consumer_mock(end_offsets: dict, committed: dict, topic_partitions=None):
    """Build a mock KafkaConsumer whose relevant methods return canned data."""
    consumer = MagicMock()
    consumer.partitions_for_topic.return_value = topic_partitions
    consumer.end_offsets.return_value = end_offsets
    consumer.committed.side_effect = lambda tp: committed.get(tp, 0)
    consumer.assignment.return_value = list(end_offsets.keys())
    return consumer


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------

def test_missing_group_id_returns_unknown(backend):
    result = backend.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.UNKNOWN
    assert "group_id" in result.message


def test_healthy_when_lag_within_threshold(backend):
    from kafka.structs import TopicPartition  # type: ignore

    tp = TopicPartition("orders", 0)
    consumer = _make_consumer_mock(
        end_offsets={tp: 100},
        committed={tp: 98},
        topic_partitions={0},
    )
    with patch("pipewatch.backends.kafka.KafkaConsumer", return_value=consumer):
        result = backend.check_pipeline(_pipeline(group_id="grp", topic="orders", max_lag=5))

    assert result.status == PipelineStatus.HEALTHY
    assert "2" in result.message  # lag value mentioned


def test_failed_when_lag_exceeds_threshold(backend):
    from kafka.structs import TopicPartition  # type: ignore

    tp = TopicPartition("orders", 0)
    consumer = _make_consumer_mock(
        end_offsets={tp: 200},
        committed={tp: 100},
        topic_partitions={0},
    )
    with patch("pipewatch.backends.kafka.KafkaConsumer", return_value=consumer):
        result = backend.check_pipeline(_pipeline(group_id="grp", topic="orders", max_lag=50))

    assert result.status == PipelineStatus.FAILED
    assert "100" in result.message


def test_unknown_when_topic_not_found(backend):
    consumer = _make_consumer_mock(end_offsets={}, committed={}, topic_partitions=None)
    with patch("pipewatch.backends.kafka.KafkaConsumer", return_value=consumer):
        result = backend.check_pipeline(_pipeline(group_id="grp", topic="missing"))

    assert result.status == PipelineStatus.UNKNOWN
    assert "missing" in result.message


def test_unknown_on_exception(backend):
    with patch(
        "pipewatch.backends.kafka.KafkaConsumer", side_effect=Exception("connection refused")
    ):
        result = backend.check_pipeline(_pipeline(group_id="grp", topic="orders"))

    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_zero_lag_healthy_with_default_threshold(backend):
    from kafka.structs import TopicPartition  # type: ignore

    tp = TopicPartition("events", 0)
    consumer = _make_consumer_mock(
        end_offsets={tp: 50},
        committed={tp: 50},
        topic_partitions={0},
    )
    with patch("pipewatch.backends.kafka.KafkaConsumer", return_value=consumer):
        result = backend.check_pipeline(_pipeline(group_id="grp", topic="events"))

    assert result.status == PipelineStatus.HEALTHY
