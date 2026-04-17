"""InfluxDB backend for pipewatch."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class InfluxDBBackend(BaseBackend):
    """Check pipeline health via an InfluxDB Flux query."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.url = config["url"].rstrip("/")
        self.token = config["token"]
        self.org = config["org"]
        self.timeout = config.get("timeout", 10)

    def check_pipeline(self, pipeline) -> PipelineResult:
        query = pipeline.options.get("query")
        if not query:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No 'query' specified in pipeline options",
            )
        threshold = pipeline.options.get("threshold", 1)
        try:
            import requests

            headers = {
                "Authorization": f"Token {self.token}",
                "Content-Type": "application/vnd.flux",
                "Accept": "application/csv",
            }
            resp = requests.post(
                f"{self.url}/api/v2/query",
                params={"org": self.org},
                headers=headers,
                data=query,
                timeout=self.timeout,
            )
            resp.raise_for_status()
            lines = [l for l in resp.text.strip().splitlines() if l and not l.startswith("#")]
            # CSV: last non-empty, non-header line; value is last column
            data_lines = lines[1:]  # skip header
            if not data_lines:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.UNKNOWN,
                    message="Query returned no results",
                )
            value = float(data_lines[-1].split(",")[-1])
            if value >= threshold:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.HEALTHY,
                    message=f"value={value} >= threshold={threshold}",
                )
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.FAILED,
                message=f"value={value} < threshold={threshold}",
            )
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=str(exc),
            )
