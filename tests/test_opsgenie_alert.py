"""Tests for the OpsGenie alert channel."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.alerts.opsgenie import OpsGenieAlertChannel, OPSGENIE_ALERTS_URL
from pipewatch.backends.base import PipelineResult, PipelineStatus


@pytest.fixture()
def healthy_result() -> PipelineResult:
    return PipelineResult(
        pipeline_name="orders_etl",
        status=PipelineStatus.HEALTHY,
        details="All rows present",
    )


@pytest.fixture()
def failing_result() -> PipelineResult:
    return PipelineResult(
        pipeline_name="orders_etl",
        status=PipelineStatus.FAILED,
        details="Row count below threshold",
    )


@pytest.fixture()
def channel() -> OpsGenieAlertChannel:
    return OpsGenieAlertChannel(api_key="test-key", priority="P2", tags=["etl", "prod"])


def test_channel_name(channel: OpsGenieAlertChannel) -> None:
    assert channel.name == "opsgenie"


def test_build_payload_failing_uses_configured_priority(
    channel: OpsGenieAlertChannel, failing_result: PipelineResult
) -> None:
    payload = channel._build_payload(failing_result)
    assert payload["priority"] == "P2"


def test_build_payload_healthy_uses_p5(
    channel: OpsGenieAlertChannel, healthy_result: PipelineResult
) -> None:
    payload = channel._build_payload(healthy_result)
    assert payload["priority"] == "P5"


def test_build_payload_contains_pipeline_name(
    channel: OpsGenieAlertChannel, failing_result: PipelineResult
) -> None:
    payload = channel._build_payload(failing_result)
    assert "orders_etl" in payload["message"]
    assert "orders_etl" in payload["alias"]


def test_build_payload_includes_tags(
    channel: OpsGenieAlertChannel, failing_result: PipelineResult
) -> None:
    payload = channel._build_payload(failing_result)
    assert "etl" in payload["tags"]
    assert "prod" in payload["tags"]


def test_build_payload_healthy_has_close_action(
    channel: OpsGenieAlertChannel, healthy_result: PipelineResult
) -> None:
    payload = channel._build_payload(healthy_result)
    assert payload.get("action") == "close"


def test_send_posts_to_opsgenie(channel: OpsGenieAlertChannel, failing_result: PipelineResult) -> None:
    mock_resp = MagicMock()
    mock_resp.raise_for_status.return_value = None

    with patch("pipewatch.alerts.opsgenie.requests.post", return_value=mock_resp) as mock_post:
        channel.send(failing_result)

    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args
    assert call_kwargs[0][0] == OPSGENIE_ALERTS_URL
    assert call_kwargs[1]["headers"]["Authorization"] == "GenieKey test-key"


def test_send_logs_error_on_request_exception(
    channel: OpsGenieAlertChannel, failing_result: PipelineResult
) -> None:
    import requests as req

    with patch("pipewatch.alerts.opsgenie.requests.post", side_effect=req.RequestException("timeout")):
        # Should not raise
        channel.send(failing_result)
