"""Elasticsearch backend for pipewatch."""

from __future__ import annotations

from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class ElasticsearchBackend(BaseBackend):
    """Check pipeline health by querying an Elasticsearch index."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._url: str = config.get("url", "http://localhost:9200")
        self._index: str = config.get("index", "")
        self._query: dict[str, Any] = config.get("query", {"match_all": {}})
        self._threshold: int = int(config.get("threshold", 1))

    def check_pipeline(self, pipeline_name: str, pipeline_config: dict[str, Any]) -> PipelineResult:
        """Query Elasticsearch hit count and compare against threshold."""
        try:
            import requests
        except ImportError as exc:  # pragma: no cover
            raise ImportError("requests is required for ElasticsearchBackend") from exc

        index = pipeline_config.get("index", self._index)
        query = pipeline_config.get("query", self._query)
        threshold = int(pipeline_config.get("threshold", self._threshold))

        url = f"{self._url}/{index}/_count"
        try:
            response = requests.post(url, json={"query": query}, timeout=10)
            response.raise_for_status()
            count = response.json().get("count", 0)
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                pipeline_name=pipeline_name,
                status=PipelineStatus.UNKNOWN,
                message=f"Elasticsearch error: {exc}",
            )

        if count >= threshold:
            return PipelineResult(
                pipeline_name=pipeline_name,
                status=PipelineStatus.HEALTHY,
                message=f"count={count} meets threshold={threshold}",
            )
        return PipelineResult(
            pipeline_name=pipeline_name,
            status=PipelineStatus.FAILED,
            message=f"count={count} below threshold={threshold}",
        )
