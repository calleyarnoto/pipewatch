"""Tests for the RabbitMQ backend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.rabbitmq import RabbitMQBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    return RabbitMQBackend(
        {"base_url": "http://rabbit:15672", "username": "user", "password": "pass"}
    )


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(
        name="orders_queue",
        options={"queue": "orders", "threshold": "5"},
    )


def _mock_response(messages: int) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {"messages": messages}
    resp.raise_for_status.return_value = None
    return resp


def test_healthy_when_depth_meets_threshold(backend, _pipeline):
    with patch("pipewatch.backends.rabbitmq.requests.get", return_value=_mock_response(10)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_failed_when_depth_below_threshold(backend, _pipeline):
    with patch("pipewatch.backends.rabbitmq.requests.get", return_value=_mock_response(2)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_message_includes_queue_name(backend, _pipeline):
    with patch("pipewatch.backends.rabbitmq.requests.get", return_value=_mock_response(10)):
        result = backend.check_pipeline(_pipeline)
    assert "orders" in result.message


def test_unknown_when_queue_option_missing(backend):
    pipeline = SimpleNamespace(name="no_queue", options={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "queue" in result.message


def test_unknown_on_request_exception(backend, _pipeline):
    with patch(
        "pipewatch.backends.rabbitmq.requests.get", side_effect=ConnectionError("refused")
    ):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "refused" in result.message


def test_default_threshold_is_one(backend):
    pipeline = SimpleNamespace(name="p", options={"queue": "q"})
    with patch("pipewatch.backends.rabbitmq.requests.get", return_value=_mock_response(0)):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.FAILED

    with patch("pipewatch.backends.rabbitmq.requests.get", return_value=_mock_response(1)):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.HEALTHY
