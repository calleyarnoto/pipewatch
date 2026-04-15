from __future__ import annotations

import logging
from typing import Any

import requests

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)


class DatabricksBackend(BaseBackend):
    """Backend that checks Databricks job run health via the Jobs API."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._host = config["host"].rstrip("/")
        self._token = config["token"]
        self._headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
        }

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        job_id = pipeline.params.get("job_id")
        if not job_id:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'job_id' is required in pipeline params",
            )

        url = f"{self._host}/api/2.1/jobs/runs/list"
        params = {"job_id": job_id, "limit": 1, "expand_tasks": False}

        try:
            response = requests.get(url, headers=self._headers, params=params, timeout=10)
            response.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Databricks API error for %s: %s", pipeline.name, exc)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"API request failed: {exc}",
            )

        data = response.json()
        runs = data.get("runs", [])
        if not runs:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No runs found for job",
            )

        latest = runs[0]
        life_cycle = latest.get("state", {}).get("life_cycle_state", "")
        result_state = latest.get("state", {}).get("result_state", "")

        if life_cycle in ("RUNNING", "PENDING", "BLOCKED"):
            status = PipelineStatus.UNKNOWN
            message = f"Job is {life_cycle.lower()}"
        elif result_state == "SUCCESS":
            status = PipelineStatus.HEALTHY
            message = "Last run succeeded"
        else:
            status = PipelineStatus.FAILED
            message = f"Last run result: {result_state or life_cycle}"

        return PipelineResult(
            pipeline_name=pipeline.name,
            status=status,
            message=message,
        )
