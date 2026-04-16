"""Datadog metrics backend for pipewatch."""
from __future__ import annotations

import logging
from typing import Any

import requests

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)

DATADOG_API_BASE = "https://api.datadoghq.com/api/v1"


class DatadogBackend(BaseBackend):
    """Check pipeline health via a Datadog metric query."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._api_key = config.get("api_key", "")
        self._app_key = config.get("app_key", "")
        self._base_url = config.get("base_url", DATADOG_API_BASE).rstrip("/")

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        query: str = pipeline.options.get("query", "")
        threshold: float = float(pipeline.options.get("threshold", 1.0))

        if not query:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No 'query' specified in pipeline options",
            )

        headers = {
            "DD-API-KEY": self._api_key,
            "DD-APPLICATION-KEY": self._app_key,
        }

        try:
            resp = requests.get(
                f"{self._base_url}/query",
                headers=headers,
                params={"query": query, "from": "now-5m", "to": "now"},
                timeout=10,
            )
            resp.raise_for_status()
            data = resp.json()
            series = data.get("series", [])
            if not series:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.UNKNOWN,
                    message="No series returned by Datadog query",
                )
            pointlist = series[0].get("pointlist", [])
            if not pointlist:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.UNKNOWN,
                    message="Empty pointlist in Datadog series",
                )
            latest_value = pointlist[-1][1]
            if latest_value is None:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.UNKNOWN,
                    message="Latest metric value is null",
                )
            healthy = float(latest_value) >= threshold
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.HEALTHY if healthy else PipelineStatus.FAILED,
                message=f"Latest value {latest_value} {'>=': '>='} threshold {threshold}"
                if healthy
                else f"Latest value {latest_value} < threshold {threshold}",
            )
        except requests.RequestException as exc:
            logger.warning("Datadog request failed for %s: %s", pipeline.name, exc)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Request error: {exc}",
            )
