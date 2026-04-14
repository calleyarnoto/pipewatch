"""Tests for the Microsoft Teams alert channel."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts import AlertMessage
from pipewatch.alerts.teams import TeamsAlertChannel
from pipewatch.backends.base import PipelineResult, PipelineStatus

WEBHOOK_URL = "https://outlook.office.com/webhook/fake-url"


@pytest.fixture()
def healthy_result() -> PipelineResult:
    return PipelineResult(
        pipeline_name="orders_etl",
        status=PipelineStatus.HEALTHY,
        message="All rows loaded successfully.",
    )


@pytest.fixture()
def failing_result() -> PipelineResult:
    return PipelineResult(
        pipeline_name="orders_etl",
        status=PipelineStatus.FAILED,
        message="Row count below threshold.",
    )


@pytest.fixture()
def channel() -> TeamsAlertChannel:
    return TeamsAlertChannel(webhook_url=WEBHOOK_URL)


def test_channel_name(channel: TeamsAlertChannel) -> None:
    assert channel.name == "teams"


def test_build_payload_healthy(channel: TeamsAlertChannel, healthy_result: PipelineResult) -> None:
    payload = channel._build_payload(healthy_result)
    assert payload["themeColor"] == "00C853"
    facts = payload["sections"][0]["facts"]
    statuses = {f["name"]: f["value"] for f in facts}
    assert statuses["Status"] == "HEALTHY"
    assert statuses["Pipeline"] == "orders_etl"


def test_build_payload_failing(channel: TeamsAlertChannel, failing_result: PipelineResult) -> None:
    payload = channel._build_payload(failing_result)
    assert payload["themeColor"] == "D50000"
    facts = payload["sections"][0]["facts"]
    statuses = {f["name"]: f["value"] for f in facts}
    assert statuses["Status"] == "FAILED"


def test_build_payload_includes_message(channel: TeamsAlertChannel, failing_result: PipelineResult) -> None:
    payload = channel._build_payload(failing_result)
    facts = {f["name"]: f["value"] for f in payload["sections"][0]["facts"]}
    assert "Row count below threshold." in facts["Message"]


def test_send_posts_to_webhook(channel: TeamsAlertChannel, failing_result: PipelineResult) -> None:
    alert = AlertMessage(result=failing_result)
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None

    with patch("pipewatch.alerts.teams.requests.post", return_value=mock_response) as mock_post:
        channel.send(alert)

    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    assert call_kwargs[0][0] == WEBHOOK_URL
    assert "sections" in call_kwargs[1]["json"]


def test_send_logs_error_on_failure(channel: TeamsAlertChannel, failing_result: PipelineResult) -> None:
    import requests as req

    alert = AlertMessage(result=failing_result)

    with patch("pipewatch.alerts.teams.requests.post", side_effect=req.RequestException("timeout")):
        # Should not raise; errors are logged instead.
        channel.send(alert)
