"""Tests for the Google Cloud Pub/Sub backend."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.base import PipelineStatus
from pipewatch.backends.pubsub import PubSubBackend


@pytest.fixture()
def backend():
    return PubSubBackend(config={"project": "my-project"})


@pytest.fixture()
def _pipeline():
    """Factory helper – returns a pipeline stub."""

    def _make(extra=None):
        cfg = {"project": "my-project", "subscription": "my-sub"}
        if extra:
            cfg.update(extra)
        return SimpleNamespace(name="test-pipeline", config=cfg)

    return _make


def _make_sub_mock(backlog: int = 0):
    sub = MagicMock()
    sub.num_undelivered_messages = backlog
    return sub


@patch("pipewatch.backends.pubsub.pubsub_v1", create=True)
def test_healthy_when_backlog_is_zero(mock_pubsub, backend, _pipeline):
    mock_client = MagicMock()
    mock_client.__enter__ = lambda s: s
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get_subscription.return_value = _make_sub_mock(backlog=0)
    mock_pubsub.SubscriberClient.return_value = mock_client

    result = backend.check_pipeline(_pipeline())

    assert result.status == PipelineStatus.HEALTHY
    assert "backlog=0" in result.message


@patch("pipewatch.backends.pubsub.pubsub_v1", create=True)
def test_failed_when_backlog_exceeds_threshold(mock_pubsub, backend, _pipeline):
    mock_client = MagicMock()
    mock_client.__enter__ = lambda s: s
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get_subscription.return_value = _make_sub_mock(backlog=50)
    mock_pubsub.SubscriberClient.return_value = mock_client

    result = backend.check_pipeline(_pipeline())

    assert result.status == PipelineStatus.FAILED
    assert "backlog=50" in result.message


@patch("pipewatch.backends.pubsub.pubsub_v1", create=True)
def test_healthy_with_custom_threshold(mock_pubsub, backend, _pipeline):
    mock_client = MagicMock()
    mock_client.__enter__ = lambda s: s
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get_subscription.return_value = _make_sub_mock(backlog=10)
    mock_pubsub.SubscriberClient.return_value = mock_client

    result = backend.check_pipeline(_pipeline(extra={"threshold": 20}))

    assert result.status == PipelineStatus.HEALTHY


@patch("pipewatch.backends.pubsub.pubsub_v1", create=True)
def test_threshold_minus_one_skips_backlog_check(mock_pubsub, backend, _pipeline):
    mock_client = MagicMock()
    mock_client.__enter__ = lambda s: s
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get_subscription.return_value = _make_sub_mock(backlog=9999)
    mock_pubsub.SubscriberClient.return_value = mock_client

    result = backend.check_pipeline(_pipeline(extra={"threshold": -1}))

    assert result.status == PipelineStatus.HEALTHY
    assert "reachable" in result.message


@patch("pipewatch.backends.pubsub.pubsub_v1", create=True)
def test_unknown_on_api_error(mock_pubsub, backend, _pipeline):
    from google.api_core.exceptions import GoogleAPICallError

    mock_client = MagicMock()
    mock_client.__enter__ = lambda s: s
    mock_client.__exit__ = MagicMock(return_value=False)
    mock_client.get_subscription.side_effect = GoogleAPICallError("not found")
    mock_pubsub.SubscriberClient.return_value = mock_client

    result = backend.check_pipeline(_pipeline())

    assert result.status == PipelineStatus.UNKNOWN
    assert "API error" in result.message


def test_unknown_when_subscription_missing(backend):
    pipeline = SimpleNamespace(name="no-sub", config={"project": "p"})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "required" in result.message


def test_unknown_when_project_missing(backend):
    pipeline = SimpleNamespace(name="no-proj", config={"subscription": "s"})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
