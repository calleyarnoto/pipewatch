"""Tests for the Discord alert channel."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts.discord import DiscordAlertChannel
from pipewatch.backends.base import PipelineResult, PipelineStatus

WEBHOOK_URL = "https://discord.com/api/webhooks/123/abc"


@pytest.fixture()
def healthy_result() -> PipelineResult:
    return PipelineResult(pipeline_name="orders_etl", status=PipelineStatus.HEALTHY)


@pytest.fixture()
def failing_result() -> PipelineResult:
    return PipelineResult(
        pipeline_name="orders_etl",
        status=PipelineStatus.FAILED,
        message="Row count dropped to 0",
    )


@pytest.fixture()
def channel() -> DiscordAlertChannel:
    return DiscordAlertChannel(webhook_url=WEBHOOK_URL)


def test_channel_name(channel: DiscordAlertChannel) -> None:
    assert channel.name == "discord"


def test_build_payload_healthy(channel: DiscordAlertChannel, healthy_result: PipelineResult) -> None:
    payload = channel._build_payload(healthy_result)
    assert payload["username"] == "pipewatch"
    embeds = payload["embeds"]
    assert len(embeds) == 1
    assert "orders_etl" in embeds[0]["title"]
    assert embeds[0]["color"] == 0x2ECC71


def test_build_payload_failing_color(channel: DiscordAlertChannel, failing_result: PipelineResult) -> None:
    payload = channel._build_payload(failing_result)
    assert payload["embeds"][0]["color"] == 0xE74C3C


def test_build_payload_contains_status_field(channel: DiscordAlertChannel, failing_result: PipelineResult) -> None:
    payload = channel._build_payload(failing_result)
    fields = payload["embeds"][0]["fields"]
    status_field = next((f for f in fields if f["name"] == "Status"), None)
    assert status_field is not None
    assert status_field["value"] == PipelineStatus.FAILED.value


def test_build_payload_description_contains_pipeline_name(
    channel: DiscordAlertChannel, failing_result: PipelineResult
) -> None:
    payload = channel._build_payload(failing_result)
    assert "orders_etl" in payload["embeds"][0]["description"]


def test_send_posts_to_webhook(channel: DiscordAlertChannel, healthy_result: PipelineResult) -> None:
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    with patch("pipewatch.alerts.discord.requests.post", return_value=mock_response) as mock_post:
        channel.send(healthy_result)
        mock_post.assert_called_once()
        call_kwargs = mock_post.call_args
        assert call_kwargs[0][0] == WEBHOOK_URL


def test_send_logs_error_on_request_exception(
    channel: DiscordAlertChannel, healthy_result: PipelineResult
) -> None:
    import requests as req
    with patch("pipewatch.alerts.discord.requests.post", side_effect=req.RequestException("timeout")):
        # Should not raise
        channel.send(healthy_result)


def test_custom_username_in_payload(healthy_result: PipelineResult) -> None:
    channel = DiscordAlertChannel(webhook_url=WEBHOOK_URL, username="my-bot")
    payload = channel._build_payload(healthy_result)
    assert payload["username"] == "my-bot"
