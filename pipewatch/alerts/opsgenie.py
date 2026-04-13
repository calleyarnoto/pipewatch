"""OpsGenie alert channel for pipewatch."""

from __future__ import annotations

import logging
from typing import Any, Dict

import requests

from pipewatch.alerts import BaseAlertChannel, AlertMessage
from pipewatch.backends.base import PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)

OPSGENIE_ALERTS_URL = "https://api.opsgenie.com/v2/alerts"


class OpsGenieAlertChannel(BaseAlertChannel):
    """Send alerts to OpsGenie via the REST API."""

    def __init__(self, api_key: str, priority: str = "P3", tags: list[str] | None = None) -> None:
        self._api_key = api_key
        self._priority = priority
        self._tags = tags or []

    @property
    def name(self) -> str:
        return "opsgenie"

    def _build_payload(self, result: PipelineResult) -> Dict[str, Any]:
        msg = AlertMessage(result)
        is_failing = result.status == PipelineStatus.FAILED

        payload: Dict[str, Any] = {
            "message": msg.format(),
            "alias": f"pipewatch-{result.pipeline_name}",
            "description": (
                f"Pipeline '{result.pipeline_name}' is in state: {result.status.value}."
                f" Details: {result.details or 'N/A'}"
            ),
            "priority": self._priority if is_failing else "P5",
            "tags": self._tags,
            "source": "pipewatch",
        }

        if not is_failing:
            # Resolve the alert when the pipeline is healthy
            payload["action"] = "close"

        return payload

    def send(self, result: PipelineResult) -> None:
        payload = self._build_payload(result)
        headers = {
            "Authorization": f"GenieKey {self._api_key}",
            "Content-Type": "application/json",
        }
        try:
            resp = requests.post(OPSGENIE_ALERTS_URL, json=payload, headers=headers, timeout=10)
            resp.raise_for_status()
            logger.info("OpsGenie alert sent for pipeline '%s'", result.pipeline_name)
        except requests.RequestException as exc:
            logger.error("Failed to send OpsGenie alert: %s", exc)
