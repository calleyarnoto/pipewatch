"""Tests for pipewatch alert channels."""

import pytest

from pipewatch.alerts import (
    AlertMessage,
    LogAlertChannel,
    build_alert_from_result,
    get_alert_channel,
)
from pipewatch.backends.base import PipelineResult, PipelineStatus


@pytest.fixture
def healthy_result():
    return PipelineResult(
        pipeline_name="etl_daily",
        status=PipelineStatus.OK,
        message="All checks passed",
    )


@pytest.fixture
def failing_result():
    return PipelineResult(
        pipeline_name="etl_hourly",
        status=PipelineStatus.CRITICAL,
        message="Last run exceeded threshold",
    )


@pytest.fixture
def log_channel():
    return LogAlertChannel()


def test_alert_message_format_includes_pipeline_name():
    alert = AlertMessage(pipeline_name="my_pipe", status="critical", message="Failed")
    formatted = alert.format()
    assert "my_pipe" in formatted


def test_alert_message_format_includes_status():
    alert = AlertMessage(pipeline_name="my_pipe", status="critical", message="Failed")
    formatted = alert.format()
    assert "critical" in formatted


def test_alert_message_format_includes_details_when_present():
    alert = AlertMessage(
        pipeline_name="my_pipe",
        status="warning",
        message="Slow run",
        details="Took 300s",
    )
    formatted = alert.format()
    assert "Took 300s" in formatted


def test_alert_message_format_no_details_when_absent():
    alert = AlertMessage(pipeline_name="my_pipe", status="ok", message="All good")
    formatted = alert.format()
    assert "Details" not in formatted


def test_log_channel_send_returns_true(log_channel):
    alert = AlertMessage(pipeline_name="p", status="ok", message="fine")
    assert log_channel.send(alert) is True


def test_log_channel_records_sent_alerts(log_channel):
    alert = AlertMessage(pipeline_name="p", status="ok", message="fine")
    log_channel.send(alert)
    assert len(log_channel.sent_alerts) == 1
    assert log_channel.sent_alerts[0].pipeline_name == "p"


def test_log_channel_name(log_channel):
    assert log_channel.name() == "log"


def test_build_alert_from_healthy_result(healthy_result):
    alert = build_alert_from_result(healthy_result)
    assert alert.pipeline_name == "etl_daily"
    assert alert.status == "ok"
    assert alert.details == "All checks passed"


def test_build_alert_from_failing_result(failing_result):
    alert = build_alert_from_result(failing_result)
    assert alert.pipeline_name == "etl_hourly"
    assert alert.status == "critical"


def test_get_alert_channel_returns_log_channel():
    channel = get_alert_channel("log")
    assert isinstance(channel, LogAlertChannel)


def test_get_alert_channel_raises_for_unknown():
    with pytest.raises(ValueError, match="Unknown alert channel"):
        get_alert_channel("slack")
