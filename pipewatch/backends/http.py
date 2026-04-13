"""HTTP backend for pipewatch — checks pipeline health via HTTP endpoints."""

from __future__ import annotations

from typing import Any

import requests
from requests.exceptions import RequestException

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class HTTPBackend(BaseBackend):
    """Check pipeline health by polling an HTTP endpoint.

    Pipeline config extras:
        url (str): The endpoint to GET. Required.
        expected_status (int): HTTP status code considered healthy. Default 200.
        timeout (float): Request timeout in seconds. Default 10.
        json_path (str): Dot-separated key path into a JSON response body.
            If provided, the resolved value must be truthy for the pipeline
            to be considered healthy.  Example: ``"data.healthy``.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self.timeout: float = float(config.get("timeout", 10))

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        extras: dict[str, Any] = pipeline.extras or {}
        url: str | None = extras.get("url")
        if not url:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No 'url' configured for HTTP backend",
            )

        expected_status: int = int(extras.get("expected_status", 200))
        timeout: float = float(extras.get("timeout", self.timeout))
        json_path: str | None = extras.get("json_path")

        try:
            response = requests.get(url, timeout=timeout)
        except RequestException as exc:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Request failed: {exc}",
            )

        if response.status_code != expected_status:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.FAILED,
                message=(
                    f"Expected HTTP {expected_status}, "
                    f"got {response.status_code}"
                ),
            )

        if json_path:
            try:
                value = _resolve_json_path(response.json(), json_path)
            except (ValueError, KeyError, TypeError) as exc:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.UNKNOWN,
                    message=f"Could not resolve json_path '{json_path}': {exc}",
                )
            if not value:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.FAILED,
                    message=f"json_path '{json_path}' resolved to falsy value: {value!r}",
                )

        return PipelineResult(
            pipeline_name=pipeline.name,
            status=PipelineStatus.HEALTHY,
            message=f"HTTP {response.status_code} OK",
        )


def _resolve_json_path(data: Any, path: str) -> Any:
    """Traverse *data* using a dot-separated *path*."""
    for key in path.split("."):
        if isinstance(data, dict):
            data = data[key]
        else:
            raise TypeError(f"Expected dict at '{key}', got {type(data).__name__}")
    return data
