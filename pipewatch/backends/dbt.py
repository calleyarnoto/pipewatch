"""dbt Cloud backend for pipewatch."""
from __future__ import annotations

import logging
from typing import Any, Dict

import requests

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)

_TERMINAL_STATUSES = {"Success": PipelineStatus.HEALTHY, "Error": PipelineStatus.FAILED, "Cancelled": PipelineStatus.FAILED}


class DBTBackend(BaseBackend):
    """Check the latest run of a dbt Cloud job."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self._account_id: int = int(config["account_id"])
        self._token: str = config["api_token"]
        self._base_url: str = config.get("base_url", "https://cloud.getdbt.com").rstrip("/")

    def check_pipeline(self, pipeline) -> PipelineResult:
        job_id = pipeline.options.get("job_id")
        if not job_id:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="job_id is required in pipeline options",
            )

        url = f"{self._base_url}/api/v2/accounts/{self._account_id}/runs/"
        params = {"job_definition_id": job_id, "order_by": "-created_at", "limit": 1}
        headers = {"Authorization": f"Token {self._token}"}

        try:
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as exc:
            logger.warning("dbt Cloud request failed: %s", exc)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=str(exc),
            )

        runs = data.get("data", [])
        if not runs:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No runs found for job",
            )

        run = runs[0]
        status_humanized: str = run.get("status_humanized", "")
        mapped = _TERMINAL_STATUSES.get(status_humanized, PipelineStatus.UNKNOWN)
        return PipelineResult(
            pipeline_name=pipeline.name,
            status=mapped,
            message=f"dbt run {run.get('id')} status: {status_humanized}",
        )
