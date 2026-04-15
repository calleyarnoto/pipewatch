"""Tests for the SMS (Twilio) alert channel."""

from __future__ import annotations

from unittest.mock import MagicMock, patch, call

import pytest

from pipewatch.backends.base import PipelineResult, PipelineStatus
from pipewatch.alerts.sms import SMSAlertChannel


@pytest.fixture()
def healthy_result() -> PipelineResult:
    return PipelineResult(
        pipeline_name="orders_etl",
        status=PipelineStatus.HEALTHY,
        message="All good",
    )


@pytest.fixture()
def failing_result() -> PipelineResult:
    return PipelineResult(
        pipeline_name="orders_etl",
        status=PipelineStatus.FAILED,
        message="Row count too low",
    )


@pytest.fixture()
def channel() -> SMSAlertChannel:
    return SMSAlertChannel(
        {
            "account_sid": "ACtest123",
            "auth_token": "token_abc",
            "from_number": "+15550001111",
            "to_numbers": ["+15559990000", "+15558880000"],
        }
    )


def test_channel_name(channel: SMSAlertChannel) -> None:
    assert channel.name == "sms"


def test_build_body_contains_pipeline_name(
    channel: SMSAlertChannel, failing_result: PipelineResult
) -> None:
    body = channel._build_body(failing_result)
    assert "orders_etl" in body


def test_build_body_contains_status(
    channel: SMSAlertChannel, failing_result: PipelineResult
) -> None:
    body = channel._build_body(failing_result)
    assert "FAILED" in body


def test_send_failure_calls_twilio_for_each_number(
    channel: SMSAlertChannel, failing_result: PipelineResult
) -> None:
    mock_client_cls = MagicMock()
    mock_client = mock_client_cls.return_value

    with patch.dict("sys.modules", {"twilio": MagicMock(), "twilio.rest": MagicMock()}):
        with patch("pipewatch.alerts.sms.Client", mock_client_cls):
            channel.send(failing_result)

    assert mock_client.messages.create.call_count == 2
    calls = mock_client.messages.create.call_args_list
    called_numbers = {c.kwargs["to"] for c in calls}
    assert called_numbers == {"+15559990000", "+15558880000"}


def test_send_healthy_skipped_by_default(
    channel: SMSAlertChannel, healthy_result: PipelineResult
) -> None:
    mock_client_cls = MagicMock()
    with patch.dict("sys.modules", {"twilio": MagicMock(), "twilio.rest": MagicMock()}):
        with patch("pipewatch.alerts.sms.Client", mock_client_cls):
            channel.send(healthy_result)

    mock_client_cls.assert_not_called()


def test_send_healthy_when_only_failures_false(
    healthy_result: PipelineResult,
) -> None:
    ch = SMSAlertChannel(
        {
            "account_sid": "ACtest",
            "auth_token": "tok",
            "from_number": "+10000000000",
            "to_numbers": ["+19999999999"],
            "only_failures": False,
        }
    )
    mock_client_cls = MagicMock()
    with patch.dict("sys.modules", {"twilio": MagicMock(), "twilio.rest": MagicMock()}):
        with patch("pipewatch.alerts.sms.Client", mock_client_cls):
            ch.send(healthy_result)

    mock_client_cls.return_value.messages.create.assert_called_once()
