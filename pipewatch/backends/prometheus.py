"""Prometheus backend for pipewatch.

Checks pipeline health by querying a Prometheus HTTP API endpoint
for a user-defined metric/query expression.
"""

from __future__ import annotations

from typing import Any

import requests

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class PrometheusBackend(BaseBackend):
    """Backend that evaluates a PromQL query to determine pipeline health.

    Configuration keys (under ``backend.options``):
        base_url (str): Prometheus base URL, e.g. ``http://localhost:9090``.
        query_template (str): PromQL template; ``{pipeline}`` is replaced with
            the pipeline name.  A non-zero scalar result means *healthy*.
        timeout (int): HTTP request timeout in seconds (default: 10).
    """

    name = "prometheus"

    def __init__(self, options: dict[str, Any]) -> None:
        self._base_url = options["base_url"].rstrip("/")
        self._query_template = options["query_template"]
        self._timeout: int = int(options.get("timeout", 10))

    def check_pipeline(self, pipeline_name: str) -> PipelineResult:
        """Query Prometheus and return a :class:`PipelineResult`."""
        query = self._query_template.replace("{pipeline}", pipeline_name)
        url = f"{self._base_url}/api/v1/query"

        try:
            response = requests.get(
                url,
                params={"query": query},
                timeout=self._timeout,
            )
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as exc:
            return PipelineResult(
                pipeline_name=pipeline_name,
                status=PipelineStatus.UNKNOWN,
                message=f"Prometheus request failed: {exc}",
            )

        results = data.get("data", {}).get("result", [])
        if not results:
            return PipelineResult(
                pipeline_name=pipeline_name,
                status=PipelineStatus.UNKNOWN,
                message="No data returned from Prometheus query.",
            )

        # Expect a scalar / instant-vector; use the first result's value.
        value = float(results[0]["value"][1])
        if value > 0:
            status = PipelineStatus.HEALTHY
            message = f"Metric value {value} indicates healthy."
        else:
            status = PipelineStatus.FAILED
            message = f"Metric value {value} indicates failure."

        return PipelineResult(
            pipeline_name=pipeline_name,
            status=status,
            message=message,
        )
