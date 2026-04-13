"""PagerDuty alert channel for pipewatch."""

from __future__ import annotations

import logging
from typing import Any

import requests

from pipewatch.alerts import BaseAlertChannel, AlertMessage
from pipewatch.backends.base import PipelineResult

logger = logging.getLogger(__name__)

_EVENTS_API_URL = "https://events.pagerduty.com/v2/enqueue"


class PagerDutyAlertChannel(BaseAlertChannel):
    """Send alerts to PagerDuty via the Events API v2."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._integration_key: str = config["integration_key"]
        self._severity: str = config.get("severity", "error")
        self._source: str = config.get("source", "pipewatch")

    @property
    def name(self) -> str:
        return "pagerduty"

    def _build_payload(self, result: PipelineResult) -> dict[str, Any]:
        msg = AlertMessage(result)
        return {
            "routing_key": self._integration_key,
            "event_action": "trigger",
            "payload": {
                "summary": msg.format(),
                "source": self._source,
                "severity": self._severity,
                "custom_details": {
                    "pipeline": result.name,
                    "status": str(result.status),
                    "message": result.message or "",
                },
            },
        }

    def send(self, result: PipelineResult) -> None:
        payload = self._build_payload(result)
        try:
            response = requests.post(_EVENTS_API_URL, json=payload, timeout=10)
            response.raise_for_status()
            logger.debug(
                "PagerDuty alert sent for pipeline '%s' (dedup_key=%s)",
                result.name,
                response.json().get("dedup_key", "unknown"),
            )
        except requests.RequestException as exc:
            logger.error("Failed to send PagerDuty alert: %s", exc)
