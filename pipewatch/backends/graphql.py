"""GraphQL backend for pipewatch.

Checks pipeline health by executing a GraphQL query and evaluating
a numeric field from the response against a threshold.
"""
from __future__ import annotations

import logging
from typing import Any

import requests

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)

_DEFAULT_THRESHOLD = 1
_DEFAULT_TIMEOUT = 10


class GraphQLBackend(BaseBackend):
    """Backend that queries a GraphQL endpoint to assess pipeline health."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._url: str = config["url"]
        self._headers: dict[str, str] = config.get("headers", {})
        self._timeout: int = int(config.get("timeout", _DEFAULT_TIMEOUT))

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        """Execute the pipeline's GraphQL query and evaluate the result."""
        extra = pipeline.extra or {}
        query = extra.get("query")
        variables = extra.get("variables", {})
        field_path: str = extra.get("field_path", "")
        threshold = int(extra.get("threshold", _DEFAULT_THRESHOLD))

        if not query or not field_path:
            logger.warning(
                "Pipeline '%s' missing 'query' or 'field_path' in extra config.",
                pipeline.name,
            )
            return PipelineResult(pipeline.name, PipelineStatus.UNKNOWN)

        try:
            response = requests.post(
                self._url,
                json={"query": query, "variables": variables},
                headers=self._headers,
                timeout=self._timeout,
            )
            response.raise_for_status()
            body = response.json()
        except requests.RequestException as exc:
            logger.error("GraphQL request failed for '%s': %s", pipeline.name, exc)
            return PipelineResult(pipeline.name, PipelineStatus.UNKNOWN)

        value = self._resolve_field(body.get("data", {}), field_path)
        if value is None:
            logger.warning(
                "Field path '%s' not found in response for '%s'.",
                field_path,
                pipeline.name,
            )
            return PipelineResult(pipeline.name, PipelineStatus.UNKNOWN)

        try:
            numeric = float(value)
        except (TypeError, ValueError):
            logger.warning(
                "Field '%s' value %r is not numeric for pipeline '%s'.",
                field_path,
                value,
                pipeline.name,
            )
            return PipelineResult(pipeline.name, PipelineStatus.UNKNOWN)

        status = PipelineStatus.HEALTHY if numeric >= threshold else PipelineStatus.FAILED
        return PipelineResult(pipeline.name, status)

    @staticmethod
    def _resolve_field(data: dict[str, Any], field_path: str) -> Any:
        """Traverse dot-separated field_path into nested dict."""
        node: Any = data
        for key in field_path.split("."):
            if not isinstance(node, dict) or key not in node:
                return None
            node = node[key]
        return node
