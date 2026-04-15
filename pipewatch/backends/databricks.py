"""Databricks backend for pipewatch.

Checks the status of a Databricks job by querying the Jobs API
for the most recent run of a given job_id.
"""

from __future__ import annotations

import logging
from typing import Any

import requests

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)

# Databricks terminal run life-cycle states that map to a healthy outcome
_HEALTHY_RESULT_STATES = {"SUCCESS"}
# Terminal states that are definitively failed
_FAILED_RESULT_STATES = {"FAILED", "TIMEDOUT", "CANCELED"}


class DatabricksBackend(BaseBackend):
    """Backend that checks Databricks job run health via the Jobs API."""

    name = "databricks"

    def __init__(self, config: dict[str, Any]) -> None:
        self._host = config["host"].rstrip("/")
        self._token = config["token"]
        self._timeout = int(config.get("timeout", 10))
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {self._token}",
                "Content-Type": "application/json",
            }
        )

    def check_pipeline(self, pipeline: dict[str, Any]) -> PipelineResult:
        """Return a PipelineResult for the latest run of the given Databricks job."""
        pipeline_name: str = pipeline["name"]
        job_id = pipeline.get("job_id")

        if not job_id:
            logger.warning("Pipeline '%s' missing required 'job_id'.", pipeline_name)
            return PipelineResult(
                name=pipeline_name,
                status=PipelineStatus.UNKNOWN,
                message="Missing required parameter: job_id",
            )

        url = f"{self._host}/api/2.1/jobs/runs/list"
        params = {"job_id": job_id, "limit": 1, "expand_tasks": False}

        try:
            response = self._session.get(url, params=params, timeout=self._timeout)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as exc:
            logger.error("Databricks API error for '%s': %s", pipeline_name, exc)
            return PipelineResult(
                name=pipeline_name,
                status=PipelineStatus.UNKNOWN,
                message=f"API error: {exc}",
            )

        runs = data.get("runs", [])
        if not runs:
            return PipelineResult(
                name=pipeline_name,
                status=PipelineStatus.UNKNOWN,
                message="No runs found for job",
            )

        latest = runs[0]
        life_cycle = latest.get("state", {}).get("life_cycle_state", "")
        result_state = latest.get("state", {}).get("result_state", "")

        if life_cycle != "TERMINATED":
            return PipelineResult(
                name=pipeline_name,
                status=PipelineStatus.UNKNOWN,
                message=f"Run not yet terminated (life_cycle_state={life_cycle})",
            )

        if result_state in _HEALTHY_RESULT_STATES:
            status = PipelineStatus.HEALTHY
        elif result_state in _FAILED_RESULT_STATES:
            status = PipelineStatus.FAILED
        else:
            status = PipelineStatus.UNKNOWN

        return PipelineResult(
            name=pipeline_name,
            status=status,
            message=f"result_state={result_state}",
        )
