"""Slack alert channel for pipewatch."""

from __future__ import annotations

import logging
from typing import Any

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None  # type: ignore[assignment]

from pipewatch.alerts import AlertMessage, BaseAlertChannel

logger = logging.getLogger(__name__)


class SlackAlertChannel(BaseAlertChannel):
    """Send pipeline alerts to a Slack webhook URL."""

    channel_name = "slack"

    def __init__(self, webhook_url: str, username: str = "pipewatch", icon_emoji: str = ":warning:") -> None:
        if requests is None:  # pragma: no cover
            raise ImportError("'requests' is required for SlackAlertChannel. Run: pip install requests")
        self._webhook_url = webhook_url
        self._username = username
        self._icon_emoji = icon_emoji

    @property
    def name(self) -> str:
        return self.channel_name

    def _build_payload(self, message: AlertMessage) -> dict[str, Any]:
        status_emoji = ":white_check_mark:" if message.result.is_healthy() else ":red_circle:"
        text = f"{status_emoji} {message.format()}"
        return {
            "username": self._username,
            "icon_emoji": self._icon_emoji,
            "text": text,
        }

    def send(self, message: AlertMessage) -> None:
        payload = self._build_payload(message)
        try:
            response = requests.post(self._webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            logger.debug("Slack alert sent for pipeline '%s'.", message.result.pipeline_name)
        except requests.exceptions.HTTPError as exc:
            logger.error(
                "Slack webhook returned HTTP %s for pipeline '%s': %s",
                exc.response.status_code if exc.response is not None else "unknown",
                message.result.pipeline_name,
                exc,
            )
        except requests.RequestException as exc:
            logger.error("Failed to send Slack alert for pipeline '%s': %s", message.result.pipeline_name, exc)
