"""Airflow backend for pipewatch — checks DAG health via the Airflow REST API."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import requests

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)

_TERMINAL_STATES = {"success", "failed", "upstream_failed", "skipped"}
_HEALTHY_STATES = {"success"}


class AirflowBackend(BaseBackend):
    """Query the Airflow stable REST API (v1) to determine DAG run health."""

    name = "airflow"

    def __init__(self, base_url: str, username: str = "", password: str = "", timeout: int = 10) -> None:
        self.base_url = base_url.rstrip("/")
        self._auth = (username, password) if username else None
        self._timeout = timeout
        self._session = requests.Session()
        if self._auth:
            self._session.auth = self._auth

    def check_pipeline(self, pipeline_id: str, **kwargs: Any) -> PipelineResult:
        """Return the result of the latest DAG run for *pipeline_id*."""
        url = f"{self.base_url}/api/v1/dags/{pipeline_id}/dagRuns"
        params = {"limit": 1, "order_by": "-execution_date"}
        try:
            resp = self._session.get(url, params=params, timeout=self._timeout)
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Airflow API request failed for %s: %s", pipeline_id, exc)
            return PipelineResult(
                pipeline_id=pipeline_id,
                status=PipelineStatus.UNKNOWN,
                message=str(exc),
            )

        data = resp.json()
        runs = data.get("dag_runs", [])
        if not runs:
            return PipelineResult(
                pipeline_id=pipeline_id,
                status=PipelineStatus.UNKNOWN,
                message="No DAG runs found",
            )

        latest = runs[0]
        state: str = latest.get("state", "unknown").lower()
        execution_date: str | None = latest.get("execution_date")
        message = f"state={state}, execution_date={execution_date}"

        if state in _HEALTHY_STATES:
            status = PipelineStatus.HEALTHY
        elif state in _TERMINAL_STATES:
            status = PipelineStatus.UNHEALTHY
        else:
            status = PipelineStatus.UNKNOWN

        return PipelineResult(pipeline_id=pipeline_id, status=status, message=message)
