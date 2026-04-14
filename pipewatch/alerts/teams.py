"""Microsoft Teams alert channel for pipewatch."""

from __future__ import annotations

import logging
from typing import Any, Dict

import requests

from pipewatch.alerts import BaseAlertChannel, AlertMessage
from pipewatch.backends.base import PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)


class TeamsAlertChannel(BaseAlertChannel):
    """Send alerts to a Microsoft Teams channel via an Incoming Webhook."""

    def __init__(self, webhook_url: str, timeout: int = 10) -> None:
        self._webhook_url = webhook_url
        self._timeout = timeout

    @property
    def name(self) -> str:
        return "teams"

    def _build_payload(self, result: PipelineResult) -> Dict[str, Any]:
        """Build an Adaptive Card-style payload for Teams."""
        is_healthy = result.status == PipelineStatus.HEALTHY
        color = "00C853" if is_healthy else "D50000"
        status_label = result.status.value.upper()

        return {
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "themeColor": color,
            "summary": f"Pipewatch alert: {result.pipeline_name}",
            "sections": [
                {
                    "activityTitle": f"Pipeline **{result.pipeline_name}** is **{status_label}**",
                    "facts": [
                        {"name": "Pipeline", "value": result.pipeline_name},
                        {"name": "Status", "value": status_label},
                        {"name": "Message", "value": result.message or "No details provided."},
                    ],
                    "markdown": True,
                }
            ],
        }

    def send(self, message: AlertMessage) -> None:
        payload = self._build_payload(message.result)
        try:
            response = requests.post(
                self._webhook_url,
                json=payload,
                timeout=self._timeout,
            )
            response.raise_for_status()
            logger.info("Teams alert sent for pipeline '%s'.", message.result.pipeline_name)
        except requests.RequestException as exc:
            logger.error("Failed to send Teams alert: %s", exc)
