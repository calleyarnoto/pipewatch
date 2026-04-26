"""Apache Spark backend — checks job status via the Spark History Server REST API."""

from __future__ import annotations

import logging
from typing import Any, Dict

import requests

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus
from pipewatch.config import PipelineConfig

logger = logging.getLogger(__name__)

_TERMINAL_SUCCEEDED = {"succeeded"}
_TERMINAL_FAILED = {"failed"}


class SparkBackend(BaseBackend):
    """Query the Spark History Server to determine whether the most recent
    application run for a given app name succeeded.

    Pipeline config extras:
        history_server (str): Base URL of the Spark History Server, e.g.
            ``http://spark-history:18080``.
        app_name (str): Spark application name to look up.
        timeout (int): HTTP request timeout in seconds (default: 10).
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self._history_server: str = config.get("history_server", "http://localhost:18080")
        self._timeout: int = int(config.get("timeout", 10))

    def check_pipeline(self, pipeline: PipelineConfig) -> PipelineResult:
        app_name: str = pipeline.extras.get("app_name", pipeline.name)
        url = f"{self._history_server.rstrip('/')}/api/v1/applications"
        params = {"limit": 20}

        try:
            resp = requests.get(url, params=params, timeout=self._timeout)
            resp.raise_for_status()
            apps = resp.json()
        except Exception as exc:  # noqa: BLE001
            logger.warning("SparkBackend request failed for %s: %s", pipeline.name, exc)
            return PipelineResult(pipeline.name, PipelineStatus.UNKNOWN)

        # Find the most recent entry whose name matches.
        match = next(
            (a for a in apps if a.get("name") == app_name),
            None,
        )
        if match is None:
            logger.debug("No Spark app found with name %r", app_name)
            return PipelineResult(pipeline.name, PipelineStatus.UNKNOWN)

        # Grab the last attempt's status.
        attempts = match.get("attempts", [])
        if not attempts:
            return PipelineResult(pipeline.name, PipelineStatus.UNKNOWN)

        last_attempt: Dict[str, Any] = attempts[0]  # History Server returns newest first.
        completed: bool = last_attempt.get("completed", False)
        duration: int = last_attempt.get("duration", 0)

        if not completed:
            return PipelineResult(pipeline.name, PipelineStatus.UNKNOWN)

        # A completed run with non-zero duration and no failure flag is healthy.
        status = (
            PipelineStatus.HEALTHY
            if duration > 0
            else PipelineStatus.FAILED
        )
        return PipelineResult(pipeline.name, status)
