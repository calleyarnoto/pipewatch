"""Tests for the VictorOps alert channel."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from pipewatch.alerts.victorops import VictorOpsAlertChannel
from pipewatch.backends.base import PipelineResult, PipelineStatus

REST_ENDPOINT = "https://alert.victorops.com/integrations/generic/20131114/alert/TOKEN"
ROUTING_KEY = "default"


@pytest.fixture()
def healthy_result() -> PipelineResult:
    return PipelineResult(pipeline_name="orders_etl", status=PipelineStatus.HEALTHY)


@pytest.fixture()
def failing_result() -> PipelineResult:
    return PipelineResult(
        pipeline_name="orders_etl",
        status=PipelineStatus.FAILED,
        message="Row count dropped to zero",
    )


@pytest.fixture()
def channel() -> VictorOpsAlertChannel:
    return VictorOpsAlertChannel(
        routing_key=ROUTING_KEY,
        rest_endpoint_url=REST_ENDPOINT,
    )


def test_channel_name(channel: VictorOpsAlertChannel) -> None:
    assert channel.name == "victorops"


def test_build_payload_healthy(channel: VictorOpsAlertChannel, healthy_result: PipelineResult) -> None:
    payload = channel._build_payload(healthy_result)
    assert payload["message_type"] == "RECOVERY"
    assert payload["entity_id"] == "pipewatch.orders_etl"
    assert "orders_etl" in payload["entity_display_name"]
    assert payload["monitoring_tool"] == "pipewatch"


def test_build_payload_failing(channel: VictorOpsAlertChannel, failing_result: PipelineResult) -> None:
    payload = channel._build_payload(failing_result)
    assert payload["message_type"] == "CRITICAL"
    assert "orders_etl" in payload["state_message"]


def test_build_payload_unknown_status(channel: VictorOpsAlertChannel) -> None:
    result = PipelineResult(pipeline_name="orders_etl", status=PipelineStatus.UNKNOWN)
    payload = channel._build_payload(result)
    assert payload["message_type"] == "WARNING"


def test_send_posts_to_correct_url(channel: VictorOpsAlertChannel, failing_result: PipelineResult) -> None:
    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    with patch("pipewatch.alerts.victorops.requests.post", return_value=mock_response) as mock_post:
        channel.send(failing_result)
        mock_post.assert_called_once()
        called_url = mock_post.call_args[0][0]
        assert ROUTING_KEY in called_url
        assert "TOKEN" in called_url


def test_send_logs_error_on_request_exception(
    channel: VictorOpsAlertChannel, failing_result: PipelineResult
) -> None:
    with patch(
        "pipewatch.alerts.victorops.requests.post",
        side_effect=requests.ConnectionError("unreachable"),
    ):
        # Should not raise; errors are logged instead
        channel.send(failing_result)
