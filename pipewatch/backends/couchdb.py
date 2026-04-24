"""CouchDB backend for pipewatch.

Checks pipeline health by querying a CouchDB view or document count
and comparing against a configurable threshold.
"""

from __future__ import annotations

from typing import Any

import requests
from requests.auth import HTTPBasicAuth

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class CouchDBBackend(BaseBackend):
    """Backend that queries a CouchDB database for document counts."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._url = config.get("url", "http://localhost:5984").rstrip("/")
        self._username = config.get("username", "")
        self._password = config.get("password", "")
        self._timeout = int(config.get("timeout", 10))

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        extra = pipeline.extra or {}
        database = extra.get("database")
        if not database:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'database' is required in pipeline extra config",
            )

        threshold = int(extra.get("threshold", 1))
        design = extra.get("design")
        view = extra.get("view")

        auth = HTTPBasicAuth(self._username, self._password) if self._username else None

        try:
            if design and view:
                endpoint = f"{self._url}/{database}/_design/{design}/_view/{view}"
                params = {"limit": 0}
                resp = requests.get(endpoint, params=params, auth=auth, timeout=self._timeout)
                resp.raise_for_status()
                count = resp.json().get("total_rows", 0)
            else:
                endpoint = f"{self._url}/{database}"
                resp = requests.get(endpoint, auth=auth, timeout=self._timeout)
                resp.raise_for_status()
                count = resp.json().get("doc_count", 0)
        except requests.RequestException as exc:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"CouchDB request failed: {exc}",
            )

        if count >= threshold:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.HEALTHY,
                message=f"doc_count={count} meets threshold={threshold}",
            )
        return PipelineResult(
            pipeline_name=pipeline.name,
            status=PipelineStatus.FAILED,
            message=f"doc_count={count} below threshold={threshold}",
        )
