"""Tests for the EmailAlertChannel."""

import pytest
from unittest.mock import patch, MagicMock

from pipewatch.alerts.email import EmailAlertChannel
from pipewatch.backends.base import PipelineResult, PipelineStatus


@pytest.fixture
def healthy_result():
    return PipelineResult(
        pipeline_name="orders_etl",
        status=PipelineStatus.HEALTHY,
        message="All good",
    )


@pytest.fixture
def failing_result():
    return PipelineResult(
        pipeline_name="orders_etl",
        status=PipelineStatus.FAILED,
        message="DAG failed",
    )


@pytest.fixture
def channel():
    return EmailAlertChannel(
        recipients=["ops@example.com", "dev@example.com"],
        sender="pipewatch@example.com",
        smtp_host="smtp.example.com",
        smtp_port=587,
        use_tls=True,
    )


def test_channel_name(channel):
    assert channel.name == "email"


def test_build_message_subject_contains_pipeline_name(channel, failing_result):
    msg = channel._build_message(failing_result)
    assert "orders_etl" in msg["Subject"]


def test_build_message_from_address(channel, healthy_result):
    msg = channel._build_message(healthy_result)
    assert msg["From"] == "pipewatch@example.com"


def test_build_message_to_recipients(channel, healthy_result):
    msg = channel._build_message(healthy_result)
    assert "ops@example.com" in msg["To"]
    assert "dev@example.com" in msg["To"]


def test_build_message_body_contains_status(channel, failing_result):
    msg = channel._build_message(failing_result)
    payload = msg.as_string()
    assert "FAILED" in payload


@patch("pipewatch.alerts.email.smtplib.SMTP")
def test_send_calls_smtp(mock_smtp_cls, channel, failing_result):
    mock_server = MagicMock()
    mock_smtp_cls.return_value = mock_server

    channel.send(failing_result)

    mock_smtp_cls.assert_called_once_with("smtp.example.com", 587)
    mock_server.starttls.assert_called_once()
    mock_server.sendmail.assert_called_once()
    mock_server.quit.assert_called_once()


@patch("pipewatch.alerts.email.smtplib.SMTP")
def test_send_with_credentials(mock_smtp_cls, failing_result):
    mock_server = MagicMock()
    mock_smtp_cls.return_value = mock_server

    ch = EmailAlertChannel(
        recipients=["ops@example.com"],
        sender="pipewatch@example.com",
        username="user",
        password="secret",
    )
    ch.send(failing_result)

    mock_server.login.assert_called_once_with("user", "secret")


@patch("pipewatch.alerts.email.smtplib.SMTP")
def test_send_raises_on_smtp_error(mock_smtp_cls, channel, failing_result):
    import smtplib
    mock_smtp_cls.side_effect = smtplib.SMTPException("connection refused")

    with pytest.raises(smtplib.SMTPException):
        channel.send(failing_result)
