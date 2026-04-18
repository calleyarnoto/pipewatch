"""RabbitMQ backend — checks queue depth via the HTTP management API."""
from __future__ import annotations

import logging
from typing import Any, Dict

import requests

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)

_DEFAULT_THRESHOLD = 1
_DEFAULT_BASE_URL = "http://localhost:15672"


class RabbitMQBackend(BaseBackend):
    """Uses the RabbitMQ Management HTTP API to measure queue depth."""

    name = "rabbitmq"

    def __init__(self, config: Dict[str, Any]) -> None:
        self._base_url = config.get("base_url", _DEFAULT_BASE_URL).rstrip("/")
        self._username = config.get("username", "guest")
        self._password = config.get("password", "guest")
        self._vhost = config.get("vhost", "%2F")

    def check_pipeline(self, pipeline) -> PipelineResult:
        queue = pipeline.options.get("queue")
        if not queue:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'queue' option is required for RabbitMQBackend",
            )

        threshold = int(pipeline.options.get("threshold", _DEFAULT_THRESHOLD))
        url = f"{self._base_url}/api/queues/{self._vhost}/{queue}"

        try:
            resp = requests.get(url, auth=(self._username, self._password), timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.warning("RabbitMQ request failed: %s", exc)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=str(exc),
            )

        depth = data.get("messages", 0)
        if depth >= threshold:
            status = PipelineStatus.HEALTHY
        else:
            status = PipelineStatus.FAILED

        return PipelineResult(
            pipeline_name=pipeline.name,
            status=status,
            message=f"queue '{queue}' depth={depth} threshold={threshold}",
        )
