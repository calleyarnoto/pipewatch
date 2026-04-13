"""Tests for the WebhookAlertChannel."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts.webhook import WebhookAlertChannel
from pipewatch.backends.base import PipelineResult, PipelineStatus


@pytest.fixture()
def healthy_result() -> PipelineResult:
    return PipelineResult(name="orders_etl", status=PipelineStatus.HEALTHY)


@pytest.fixture()
def failing_result() -> PipelineResult:
    return PipelineResult(
        name="orders_etl",
        status=PipelineStatus.FAILED,
        message="Row count dropped to zero",
    )


@pytest.fixture()
def channel() -> WebhookAlertChannel:
    return WebhookAlertChannel(url="https://hooks.example.com/notify")


def test_channel_name(channel: WebhookAlertChannel) -> None:
    assert channel.name == "webhook"


def test_build_payload_contains_pipeline_name(
    channel: WebhookAlertChannel, failing_result: PipelineResult
) -> None:
    payload = channel._build_payload(failing_result)
    assert payload["pipeline"] == "orders_etl"


def test_build_payload_contains_status(
    channel: WebhookAlertChannel, failing_result: PipelineResult
) -> None:
    payload = channel._build_payload(failing_result)
    assert payload["status"] == PipelineStatus.FAILED.value


def test_build_payload_healthy_flag(
    channel: WebhookAlertChannel, healthy_result: PipelineResult
) -> None:
    payload = channel._build_payload(healthy_result)
    assert payload["healthy"] is True


def test_send_skips_healthy_when_only_failures(
    channel: WebhookAlertChannel, healthy_result: PipelineResult
) -> None:
    with patch("urllib.request.urlopen") as mock_open:
        channel.send(healthy_result)
        mock_open.assert_not_called()


def test_send_posts_on_failure(
    channel: WebhookAlertChannel, failing_result: PipelineResult
) -> None:
    mock_resp = MagicMock()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    mock_resp.status = 200

    with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
        channel.send(failing_result)
        mock_open.assert_called_once()
        request = mock_open.call_args[0][0]
        assert request.full_url == "https://hooks.example.com/notify"
        assert request.get_method() == "POST"
        sent_body = json.loads(request.data.decode())
        assert sent_body["pipeline"] == "orders_etl"


def test_send_healthy_when_only_failures_false(
    failing_result: PipelineResult,
) -> None:
    channel = WebhookAlertChannel(
        url="https://hooks.example.com/notify", only_failures=False
    )
    healthy = PipelineResult(name="orders_etl", status=PipelineStatus.HEALTHY)
    mock_resp = MagicMock()
    mock_resp.__enter__ = lambda s: s
    mock_resp.__exit__ = MagicMock(return_value=False)
    mock_resp.status = 200

    with patch("urllib.request.urlopen", return_value=mock_resp) as mock_open:
        channel.send(healthy)
        mock_open.assert_called_once()


def test_send_logs_error_on_exception(
    channel: WebhookAlertChannel, failing_result: PipelineResult
) -> None:
    with patch("urllib.request.urlopen", side_effect=OSError("connection refused")):
        # Should not raise
        channel.send(failing_result)
