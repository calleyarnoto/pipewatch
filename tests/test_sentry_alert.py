"""Tests for SentryAlertChannel."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts.sentry import SentryAlertChannel
from pipewatch.backends.base import PipelineResult, PipelineStatus


@pytest.fixture()
def healthy_result():
    return PipelineResult(pipeline_name="etl_job", status=PipelineStatus.HEALTHY, message="ok")


@pytest.fixture()
def failing_result():
    return PipelineResult(pipeline_name="etl_job", status=PipelineStatus.FAILED, message="broke")


@pytest.fixture()
def channel():
    return SentryAlertChannel({
        "dsn": "https://abc123@o123.ingest.sentry.io/456",
        "environment": "staging",
    })


def test_channel_name(channel):
    assert channel.name == "sentry"


def test_build_payload_healthy_uses_info_level(channel, healthy_result):
    payload = channel._build_payload(healthy_result)
    assert payload["level"] == "info"


def test_build_payload_failing_uses_error_level(channel, failing_result):
    payload = channel._build_payload(failing_result)
    assert payload["level"] == "error"


def test_build_payload_contains_pipeline_name(channel, failing_result):
    payload = channel._build_payload(failing_result)
    assert payload["tags"]["pipeline"] == "etl_job"


def test_build_payload_contains_environment(channel, failing_result):
    payload = channel._build_payload(failing_result)
    assert payload["environment"] == "staging"


def test_build_payload_message_contains_pipeline_name(channel, failing_result):
    payload = channel._build_payload(failing_result)
    assert "etl_job" in payload["message"]


def test_send_posts_to_sentry(channel, failing_result):
    with patch("requests.post") as mock_post:
        channel.send(failing_result)
    mock_post.assert_called_once()
    _, kwargs = mock_post.call_args
    assert "json" in kwargs
    assert kwargs["json"]["level"] == "error"
