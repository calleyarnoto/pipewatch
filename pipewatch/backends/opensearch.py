"""OpenSearch backend for pipewatch."""
from __future__ import annotations

import logging
from typing import Any

import requests

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus
from pipewatch.config import PipelineConfig

logger = logging.getLogger(__name__)


class OpenSearchBackend(BaseBackend):
    """Check pipeline health by querying an OpenSearch index.

    Pipeline config extras:
        host (str): OpenSearch base URL (default: http://localhost:9200).
        index (str): Index or index pattern to query (required).
        query (dict): Optional OpenSearch query DSL body (default: match_all).
        threshold (int): Minimum hit count to be considered healthy (default: 1).
        username (str): HTTP Basic auth username (optional).
        password (str): HTTP Basic auth password (optional).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self._host = config.get("host", "http://localhost:9200").rstrip("/")
        self._username = config.get("username")
        self._password = config.get("password")

    def check_pipeline(self, pipeline: PipelineConfig) -> PipelineResult:
        index = pipeline.extras.get("index")
        if not index:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'index' is required in pipeline extras",
            )

        threshold = int(pipeline.extras.get("threshold", 1))
        query_body = pipeline.extras.get("query", {"query": {"match_all": {}}})
        url = f"{self._host}/{index}/_count"

        auth = None
        if self._username and self._password:
            auth = (self._username, self._password)

        try:
            response = requests.post(url, json=query_body, auth=auth, timeout=10)
            response.raise_for_status()
            count = response.json().get("count", 0)
        except requests.RequestException as exc:
            logger.warning("OpenSearch request failed for %s: %s", pipeline.name, exc)
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Request error: {exc}",
            )

        if count >= threshold:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.HEALTHY,
                message=f"count={count} meets threshold={threshold}",
            )
        return PipelineResult(
            name=pipeline.name,
            status=PipelineStatus.FAILED,
            message=f"count={count} below threshold={threshold}",
        )
