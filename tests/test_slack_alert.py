"""Tests for the Slack alert channel."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts import AlertMessage
from pipewatch.alerts.slack import SlackAlertChannel
from pipewatch.backends.base import PipelineResult, PipelineStatus

WEBHOOK_URL = "https://hooks.slack.com/services/TEST/WEBHOOK"


@pytest.fixture()
def healthy_result() -> PipelineResult:
    return PipelineResult(pipeline_name="orders_etl", status=PipelineStatus.HEALTHY)


@pytest.fixture()
def failing_result() -> PipelineResult:
    return PipelineResult(pipeline_name="orders_etl", status=PipelineStatus.FAILED, message="Last run failed")


@pytest.fixture()
def channel() -> SlackAlertChannel:
    return SlackAlertChannel(webhook_url=WEBHOOK_URL)


def test_channel_name(channel: SlackAlertChannel) -> None:
    assert channel.name == "slack"


def test_build_payload_healthy(channel: SlackAlertChannel, healthy_result: PipelineResult) -> None:
    msg = AlertMessage(result=healthy_result)
    payload = channel._build_payload(msg)
    assert ":white_check_mark:" in payload["text"]
    assert "orders_etl" in payload["text"]
    assert payload["username"] == "pipewatch"


def test_build_payload_failing(channel: SlackAlertChannel, failing_result: PipelineResult) -> None:
    msg = AlertMessage(result=failing_result)
    payload = channel._build_payload(msg)
    assert ":red_circle:" in payload["text"]
    assert "orders_etl" in payload["text"]


def test_send_posts_to_webhook(channel: SlackAlertChannel, failing_result: PipelineResult) -> None:
    msg = AlertMessage(result=failing_result)
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    with patch("pipewatch.alerts.slack.requests.post", return_value=mock_response) as mock_post:
        channel.send(msg)
    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    assert call_kwargs.args[0] == WEBHOOK_URL or call_kwargs.kwargs.get("url") == WEBHOOK_URL or mock_post.call_args[0][0] == WEBHOOK_URL


def test_send_logs_error_on_request_failure(channel: SlackAlertChannel, failing_result: PipelineResult, caplog: pytest.LogCaptureFixture) -> None:
    import requests as req
    msg = AlertMessage(result=failing_result)
    with patch("pipewatch.alerts.slack.requests.post", side_effect=req.RequestException("timeout")):
        channel.send(msg)  # should not raise
    assert "Failed to send Slack alert" in caplog.text


def test_custom_username_and_emoji() -> None:
    ch = SlackAlertChannel(webhook_url=WEBHOOK_URL, username="bot", icon_emoji=":robot_face:")
    result = PipelineResult(pipeline_name="pipe", status=PipelineStatus.FAILED)
    payload = ch._build_payload(AlertMessage(result=result))
    assert payload["username"] == "bot"
    assert payload["icon_emoji"] == ":robot_face:"
