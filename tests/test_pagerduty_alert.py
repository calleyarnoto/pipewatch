"""Tests for the PagerDuty alert channel."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from pipewatch.alerts.pagerduty import PagerDutyAlertChannel, _EVENTS_API_URL
from pipewatch.backends.base import PipelineResult, PipelineStatus


@pytest.fixture()
def healthy_result() -> PipelineResult:
    return PipelineResult(name="orders_etl", status=PipelineStatus.HEALTHY)


@pytest.fixture()
def failing_result() -> PipelineResult:
    return PipelineResult(
        name="orders_etl",
        status=PipelineStatus.FAILED,
        message="No rows loaded in the last hour",
    )


@pytest.fixture()
def channel() -> PagerDutyAlertChannel:
    return PagerDutyAlertChannel(
        {"integration_key": "abc123", "severity": "critical"}
    )


def test_channel_name(channel: PagerDutyAlertChannel) -> None:
    assert channel.name == "pagerduty"


def test_build_payload_contains_routing_key(
    channel: PagerDutyAlertChannel, failing_result: PipelineResult
) -> None:
    payload = channel._build_payload(failing_result)
    assert payload["routing_key"] == "abc123"


def test_build_payload_event_action_is_trigger(
    channel: PagerDutyAlertChannel, failing_result: PipelineResult
) -> None:
    payload = channel._build_payload(failing_result)
    assert payload["event_action"] == "trigger"


def test_build_payload_summary_includes_pipeline_name(
    channel: PagerDutyAlertChannel, failing_result: PipelineResult
) -> None:
    payload = channel._build_payload(failing_result)
    assert "orders_etl" in payload["payload"]["summary"]


def test_build_payload_severity_from_config(
    channel: PagerDutyAlertChannel, failing_result: PipelineResult
) -> None:
    payload = channel._build_payload(failing_result)
    assert payload["payload"]["severity"] == "critical"


def test_build_payload_default_severity() -> None:
    ch = PagerDutyAlertChannel({"integration_key": "xyz"})
    result = PipelineResult(name="p", status=PipelineStatus.FAILED)
    payload = ch._build_payload(result)
    assert payload["payload"]["severity"] == "error"


def test_send_posts_to_events_api(
    channel: PagerDutyAlertChannel, failing_result: PipelineResult
) -> None:
    mock_response = MagicMock()
    mock_response.json.return_value = {"dedup_key": "dedup-001"}
    with patch("pipewatch.alerts.pagerduty.requests.post", return_value=mock_response) as mock_post:
        channel.send(failing_result)
    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    assert call_kwargs[0][0] == _EVENTS_API_URL


def test_send_logs_error_on_request_exception(
    channel: PagerDutyAlertChannel, failing_result: PipelineResult
) -> None:
    with patch(
        "pipewatch.alerts.pagerduty.requests.post",
        side_effect=requests.RequestException("timeout"),
    ):
        # Should not raise; errors are logged instead
        channel.send(failing_result)
