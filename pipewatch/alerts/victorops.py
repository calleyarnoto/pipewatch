"""VictorOps (Splunk On-Call) alert channel for pipewatch."""

from __future__ import annotations

import logging
from typing import Any

import requests

from pipewatch.alerts import BaseAlertChannel, AlertMessage
from pipewatch.backends.base import PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)

_MESSAGE_TYPE_MAP = {
    PipelineStatus.HEALTHY: "RECOVERY",
    PipelineStatus.FAILED: "CRITICAL",
    PipelineStatus.UNKNOWN: "WARNING",
}


class VictorOpsAlertChannel(BaseAlertChannel):
    """Send alerts to VictorOps (Splunk On-Call) via the REST endpoint."""

    def __init__(self, routing_key: str, rest_endpoint_url: str, timeout: int = 10) -> None:
        """
        Args:
            routing_key: VictorOps routing key that determines escalation policy.
            rest_endpoint_url: The VictorOps REST endpoint URL (without trailing slash).
            timeout: HTTP request timeout in seconds.
        """
        self._routing_key = routing_key
        self._rest_endpoint_url = rest_endpoint_url.rstrip("/")
        self._timeout = timeout

    @property
    def name(self) -> str:
        return "victorops"

    def _build_payload(self, result: PipelineResult) -> dict[str, Any]:
        message_type = _MESSAGE_TYPE_MAP.get(result.status, "WARNING")
        alert_msg = AlertMessage(result)
        return {
            "message_type": message_type,
            "entity_id": f"pipewatch.{result.pipeline_name}",
            "entity_display_name": f"Pipeline: {result.pipeline_name}",
            "state_message": alert_msg.format(),
            "monitoring_tool": "pipewatch",
        }

    def send(self, result: PipelineResult) -> None:
        url = f"{self._rest_endpoint_url}/{self._routing_key}"
        payload = self._build_payload(result)
        try:
            response = requests.post(url, json=payload, timeout=self._timeout)
            response.raise_for_status()
            logger.info(
                "VictorOps alert sent for pipeline '%s' (message_type=%s)",
                result.pipeline_name,
                payload["message_type"],
            )
        except requests.RequestException as exc:
            logger.error("Failed to send VictorOps alert: %s", exc)
