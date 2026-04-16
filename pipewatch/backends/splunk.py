"""Splunk backend — checks pipeline health via Splunk search API."""
from __future__ import annotations

import logging
import requests

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)


class SplunkBackend(BaseBackend):
    """Query a Splunk instance using the REST search API.

    Pipeline config extras:
        query       (str)  Splunk SPL search string, e.g. 'index=etl | stats count'
        field       (str)  Field name to extract from first result row (default: 'count')
        threshold   (int)  Minimum value considered healthy (default: 1)
    """

    def __init__(self, config: dict) -> None:
        self.base_url = config["base_url"].rstrip("/")
        self.token = config["token"]
        self.verify_ssl = config.get("verify_ssl", True)
        self.timeout = config.get("timeout", 30)

    def check_pipeline(self, pipeline) -> PipelineResult:
        query = pipeline.extras.get("query")
        if not query:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No 'query' specified in pipeline config",
            )

        field = pipeline.extras.get("field", "count")
        threshold = int(pipeline.extras.get("threshold", 1))

        search_url = f"{self.base_url}/services/search/jobs/export"
        headers = {"Authorization": f"Bearer {self.token}"}
        params = {
            "search": f"search {query}",
            "output_mode": "json",
            "count": 1,
        }

        try:
            resp = requests.get(
                search_url,
                headers=headers,
                params=params,
                verify=self.verify_ssl,
                timeout=self.timeout,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            logger.warning("Splunk request failed for %s: %s", pipeline.name, exc)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=str(exc),
            )

        # Splunk export returns newline-delimited JSON; grab first result line
        import json
        rows = [line for line in resp.text.splitlines() if line.strip()]
        for raw in rows:
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if obj.get("result") and field in obj["result"]:
                try:
                    value = float(obj["result"][field])
                except (TypeError, ValueError):
                    break
                healthy = value >= threshold
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.HEALTHY if healthy else PipelineStatus.FAILED,
                    message=f"{field}={value} (threshold={threshold})",
                )

        return PipelineResult(
            pipeline_name=pipeline.name,
            status=PipelineStatus.UNKNOWN,
            message=f"Field '{field}' not found in Splunk results",
        )
