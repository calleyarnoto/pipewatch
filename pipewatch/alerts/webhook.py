"""Webhook alert channel for pipewatch."""

from __future__ import annotations

import json
import logging
import urllib.request
from typing import Any

from pipewatch.alerts import BaseAlertChannel, AlertMessage
from pipewatch.backends.base import PipelineResult

logger = logging.getLogger(__name__)


class WebhookAlertChannel(BaseAlertChannel):
    """Send alerts to a generic HTTP webhook endpoint."""

    def __init__(
        self,
        url: str,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        only_failures: bool = True,
    ) -> None:
        self._url = url
        self._method = method.upper()
        self._headers = headers or {}
        self._only_failures = only_failures

    @property
    def name(self) -> str:
        return "webhook"

    def _build_payload(self, result: PipelineResult) -> dict[str, Any]:
        msg = AlertMessage(result)
        return {
            "pipeline": result.name,
            "status": result.status.value,
            "healthy": result.is_healthy,
            "message": msg.format(),
        }

    def send(self, result: PipelineResult) -> None:
        if self._only_failures and result.is_healthy:
            logger.debug(
                "Skipping webhook alert for healthy pipeline '%s'", result.name
            )
            return

        payload = self._build_payload(result)
        body = json.dumps(payload).encode("utf-8")

        headers = {"Content-Type": "application/json", **self._headers}
        req = urllib.request.Request(
            self._url, data=body, headers=headers, method=self._method
        )

        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                logger.info(
                    "Webhook alert sent for '%s' — HTTP %s",
                    result.name,
                    resp.status,
                )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Failed to send webhook alert for '%s': %s", result.name, exc
            )
