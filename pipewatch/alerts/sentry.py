"""Sentry alert channel for pipewatch."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.alerts import BaseAlertChannel, AlertMessage
from pipewatch.backends.base import PipelineResult


class SentryAlertChannel(BaseAlertChannel):
    """Send alerts to a Sentry project via the Sentry API."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.dsn = config["dsn"]
        self.environment = config.get("environment", "production")
        self.timeout = config.get("timeout", 10)

    @property
    def name(self) -> str:
        return "sentry"

    def _build_payload(self, result: PipelineResult) -> Dict[str, Any]:
        msg = AlertMessage(result)
        level = "error" if not result.is_healthy() else "info"
        return {
            "message": msg.format(),
            "level": level,
            "environment": self.environment,
            "tags": {
                "pipeline": result.pipeline_name,
                "status": result.status.value,
            },
        }

    def send(self, result: PipelineResult) -> None:
        import requests
        from urllib.parse import urlparse

        parsed = urlparse(self.dsn)
        # DSN format: https://<key>@<host>/<project_id>
        key = parsed.username
        host = parsed.hostname
        project_id = parsed.path.strip("/")
        url = f"https://{host}/api/{project_id}/store/"
        headers = {
            "X-Sentry-Auth": (
                f"Sentry sentry_version=7, sentry_key={key}"
            ),
            "Content-Type": "application/json",
        }
        payload = self._build_payload(result)
        requests.post(url, json=payload, headers=headers, timeout=self.timeout)
