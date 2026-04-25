"""MQTT backend — checks message lag on a topic via broker stats."""
from __future__ import annotations

import logging
from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

log = logging.getLogger(__name__)

_DEFAULT_PORT = 1883
_DEFAULT_THRESHOLD = 0  # any retained message count is healthy


class MQTTBackend(BaseBackend):
    """Backend that connects to an MQTT broker and checks the number of
    retained messages (or message count) on a configured topic.

    Pipeline config extras:
        broker   (str)  – hostname of the MQTT broker (required)
        port     (int)  – broker port (default 1883)
        topic    (str)  – topic to subscribe/check (required)
        threshold (int) – minimum message count to be healthy (default 0)
        timeout  (float)– connection timeout in seconds (default 5.0)
    """

    def __init__(self, config: dict[str, Any] | None = None) -> None:
        self._config = config or {}

    def check_pipeline(self, pipeline) -> PipelineResult:
        import paho.mqtt.client as mqtt  # type: ignore

        broker = pipeline.extras.get("broker") or self._config.get("broker")
        if not broker:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'broker' is required in pipeline config",
            )

        topic = pipeline.extras.get("topic") or self._config.get("topic")
        if not topic:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'topic' is required in pipeline config",
            )

        port = int(pipeline.extras.get("port") or self._config.get("port", _DEFAULT_PORT))
        threshold = int(
            pipeline.extras.get("threshold") if pipeline.extras.get("threshold") is not None
            else self._config.get("threshold", _DEFAULT_THRESHOLD)
        )
        timeout = float(pipeline.extras.get("timeout") or self._config.get("timeout", 5.0))

        received: list[Any] = []

        def _on_message(client, userdata, msg):  # noqa: ANN001
            received.append(msg)

        client = mqtt.Client()
        client.on_message = _on_message
        try:
            client.connect(broker, port, int(timeout))
            client.subscribe(topic)
            client.loop_start()
            import time
            time.sleep(timeout)
            client.loop_stop()
            client.disconnect()
        except Exception as exc:  # noqa: BLE001
            log.warning("MQTT connection error for %s: %s", pipeline.name, exc)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"connection error: {exc}",
            )

        count = len(received)
        log.debug("MQTT topic '%s' received %d messages", topic, count)
        status = PipelineStatus.HEALTHY if count > threshold else PipelineStatus.FAILED
        return PipelineResult(
            pipeline_name=pipeline.name,
            status=status,
            message=f"received {count} messages (threshold > {threshold})",
        )
