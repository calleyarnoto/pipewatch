"""Tests for the SQS backend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.sqs import SQSBackend, _DEFAULT_THRESHOLD
from pipewatch.backends.base import PipelineStatus

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789/my-queue"


@pytest.fixture()
def backend():
    with patch("pipewatch.backends.sqs.boto3.Session") as mock_session:
        mock_session.return_value.client.return_value = MagicMock()
        b = SQSBackend({})
        b._client = mock_session.return_value.client.return_value
        yield b


def _pipeline(extra: dict | None = None):
    cfg = {"queue_url": QUEUE_URL}
    if extra:
        cfg.update(extra)
    return SimpleNamespace(name="test-queue", config=cfg)


def _set_depth(backend, depth: int):
    backend._client.get_queue_attributes.return_value = {
        "Attributes": {"ApproximateNumberOfMessages": str(depth)}
    }


def test_healthy_when_count_meets_threshold(backend):
    _set_depth(backend, _DEFAULT_THRESHOLD)
    result = backend.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.HEALTHY


def test_failed_when_count_below_threshold(backend):
    _set_depth(backend, 0)
    result = backend.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.FAILED


def test_custom_threshold(backend):
    _set_depth(backend, 5)
    result = backend.check_pipeline(_pipeline({"threshold": 10}))
    assert result.status == PipelineStatus.FAILED


def test_custom_threshold_met(backend):
    _set_depth(backend, 10)
    result = backend.check_pipeline(_pipeline({"threshold": 10}))
    assert result.status == PipelineStatus.HEALTHY


def test_missing_queue_url_returns_unknown(backend):
    pipeline = SimpleNamespace(name="no-url", config={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "queue_url" in result.message


def test_unknown_on_exception(backend):
    backend._client.get_queue_attributes.side_effect = RuntimeError("network error")
    result = backend.check_pipeline(_pipeline())
    assert result.status == PipelineStatus.UNKNOWN
    assert "network error" in result.message


def test_result_name_matches_pipeline(backend):
    _set_depth(backend, 1)
    result = backend.check_pipeline(_pipeline())
    assert result.name == "test-queue"


def test_sqs_backend_is_registered():
    from pipewatch.backends import get_backend_class
    cls = get_backend_class("sqs")
    assert cls is SQSBackend


def test_sqs_backend_name_case_insensitive():
    from pipewatch.backends import get_backend_class
    assert get_backend_class("SQS") is SQSBackend
