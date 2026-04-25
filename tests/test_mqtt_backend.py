"""Tests for the MQTT backend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.mqtt import MQTTBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend() -> MQTTBackend:
    return MQTTBackend(config={"broker": "localhost", "port": 1883})


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(
        name="test-mqtt-pipeline",
        extras={"broker": "localhost", "topic": "etl/status"},
    )


def _make_mqtt_mock(messages=None):
    """Return a patched paho.mqtt.client module where loop_start delivers messages."""
    msgs = messages if messages is not None else []

    client_instance = MagicMock()

    def fake_loop_start():
        for m in msgs:
            client_instance.on_message(client_instance, None, m)

    client_instance.loop_start.side_effect = fake_loop_start
    return client_instance


def test_missing_broker_returns_unknown():
    b = MQTTBackend()
    pipeline = SimpleNamespace(name="p", extras={})
    result = b.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "broker" in result.message


def test_missing_topic_returns_unknown():
    b = MQTTBackend(config={"broker": "localhost"})
    pipeline = SimpleNamespace(name="p", extras={})
    result = b.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "topic" in result.message


def test_healthy_when_messages_above_threshold(backend, _pipeline):
    _pipeline.extras["threshold"] = 0
    client_mock = _make_mqtt_mock(messages=[MagicMock(), MagicMock()])

    mqtt_mod = MagicMock()
    mqtt_mod.Client.return_value = client_mock

    with patch.dict("sys.modules", {"paho": MagicMock(), "paho.mqtt": MagicMock(), "paho.mqtt.client": mqtt_mod}):
        with patch("pipewatch.backends.mqtt.mqtt", mqtt_mod):
            with patch("pipewatch.backends.mqtt.time") as mock_time:
                mock_time.sleep = lambda _: None
                result = backend.check_pipeline(_pipeline)

    assert result.pipeline_name == _pipeline.name


def test_failed_when_no_messages_and_threshold_above_zero(backend, _pipeline):
    _pipeline.extras["threshold"] = 1
    client_mock = _make_mqtt_mock(messages=[])

    mqtt_mod = MagicMock()
    mqtt_mod.Client.return_value = client_mock

    with patch("pipewatch.backends.mqtt.mqtt", mqtt_mod):
        with patch("pipewatch.backends.mqtt.time") as mock_time:
            mock_time.sleep = lambda _: None
            result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.FAILED
    assert result.pipeline_name == _pipeline.name


def test_unknown_on_connection_error(backend, _pipeline):
    client_mock = MagicMock()
    client_mock.connect.side_effect = OSError("refused")

    mqtt_mod = MagicMock()
    mqtt_mod.Client.return_value = client_mock

    with patch("pipewatch.backends.mqtt.mqtt", mqtt_mod):
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.UNKNOWN
    assert "connection error" in result.message


def test_result_message_contains_count(backend, _pipeline):
    _pipeline.extras["threshold"] = 0
    client_mock = _make_mqtt_mock(messages=[MagicMock()])

    mqtt_mod = MagicMock()
    mqtt_mod.Client.return_value = client_mock

    with patch("pipewatch.backends.mqtt.mqtt", mqtt_mod):
        with patch("pipewatch.backends.mqtt.time") as mock_time:
            mock_time.sleep = lambda _: None
            result = backend.check_pipeline(_pipeline)

    assert "received" in result.message
