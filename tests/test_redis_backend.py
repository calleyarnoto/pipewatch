"""Tests for pipewatch.backends.redis.RedisBackend."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    """Return a RedisBackend with a mocked redis.Redis client."""
    with patch("pipewatch.backends.redis.redis") as mock_redis_module:
        mock_client = MagicMock()
        mock_redis_module.Redis.return_value = mock_client

        from pipewatch.backends.redis import RedisBackend

        instance = RedisBackend({"host": "localhost", "port": 6379, "threshold": 1})
        instance._redis = mock_client
        yield instance, mock_client


def _pipeline(name: str = "my_pipeline", key: str = "etl:my_pipeline", threshold: int | None = None):
    """Build a minimal pipeline SimpleNamespace for testing.

    Args:
        name: The pipeline name.
        key: The Redis key to check.
        threshold: Optional per-pipeline threshold; omitted from config when None.
    """
    cfg: dict = {"key": key}
    if threshold is not None:
        cfg["threshold"] = threshold
    return SimpleNamespace(name=name, backend_config=cfg)


def test_healthy_when_value_meets_threshold(backend):
    instance, mock_client = backend
    mock_client.get.return_value = "5"
    result = instance.check_pipeline(_pipeline(threshold=1))
    assert result.status == PipelineStatus.HEALTHY


def test_failed_when_value_below_threshold(backend):
    instance, mock_client = backend
    mock_client.get.return_value = "0"
    result = instance.check_pipeline(_pipeline(threshold=1))
    assert result.status == PipelineStatus.FAILED


def test_unknown_when_key_missing(backend):
    instance, mock_client = backend
    mock_client.get.return_value = None
    result = instance.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.UNKNOWN
    assert "does not exist" in result.message


def test_unknown_when_value_not_numeric(backend):
    instance, mock_client = backend
    mock_client.get.return_value = "not-a-number"
    result = instance.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.UNKNOWN
    assert "not numeric" in result.message


def test_unknown_when_no_key_configured(backend):
    instance, _ = backend
    pipeline = SimpleNamespace(name="no_key", backend_config={})
    result = instance.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "No Redis key" in result.message


def test_custom_threshold_per_pipeline(backend):
    instance, mock_client = backend
    mock_client.get.return_value = "3"
    result = instance.check_pipeline(_pipeline(threshold=5))
    assert result.status == PipelineStatus.FAILED


def test_result_message_contains_key_and_value(backend):
    instance, mock_client = backend
    mock_client.get.return_value = "10"
    result = instance.check_pipeline(_pipeline(key="etl:orders"))
    assert "etl:orders" in result.message
    assert "10" in result.message


def test_pipeline_name_in_result(backend):
    instance, mock_client = backend
    mock_client.get.return_value = "2"
    result = instance.check_pipeline(_pipeline(name="orders_etl"))
    assert result.pipeline_name == "orders_etl"


def test_healthy_when_value_exactly_equals_threshold(backend):
    """Boundary check: a value exactly equal to the threshold should be HEALTHY."""
    instance, mock_client = backend
    mock_client.get.return_value = "3"
    result = instance.check_pipeline(_pipeline(threshold=3))
    assert result.status == PipelineStatus.HEALTHY
