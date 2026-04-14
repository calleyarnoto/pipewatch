"""Discord alert channel for pipewatch."""

from __future__ import annotations

import logging
from typing import Any, Dict

import requests

from pipewatch.alerts import BaseAlertChannel, AlertMessage
from pipewatch.backends.base import PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)

_STATUS_COLORS: Dict[PipelineStatus, int] = {
    PipelineStatus.HEALTHY: 0x2ECC71,   # green
    PipelineStatus.FAILED: 0xE74C3C,    # red
    PipelineStatus.UNKNOWN: 0x95A5A6,   # grey
}


class DiscordAlertChannel(BaseAlertChannel):
    """Send pipeline alerts to a Discord channel via an incoming webhook."""

    def __init__(self, webhook_url: str, username: str = "pipewatch") -> None:
        self._webhook_url = webhook_url
        self._username = username

    @property
    def name(self) -> str:
        return "discord"

    def _build_payload(self, result: PipelineResult) -> Dict[str, Any]:
        msg = AlertMessage(result)
        color = _STATUS_COLORS.get(result.status, 0x95A5A6)
        return {
            "username": self._username,
            "embeds": [
                {
                    "title": f"Pipeline: {result.pipeline_name}",
                    "description": msg.format(),
                    "color": color,
                    "fields": [
                        {
                            "name": "Status",
                            "value": result.status.value,
                            "inline": True,
                        }
                    ],
                }
            ],
        }

    def send(self, result: PipelineResult) -> None:
        payload = self._build_payload(result)
        try:
            response = requests.post(self._webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            logger.debug("Discord alert sent for pipeline '%s'", result.pipeline_name)
        except requests.RequestException as exc:
            logger.error("Failed to send Discord alert: %s", exc)
