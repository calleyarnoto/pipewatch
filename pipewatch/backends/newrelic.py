"""New Relic backend for pipewatch.

Checks pipeline health by querying a NRQL metric via the New Relic Insights API.

Required pipeline config keys:
  - account_id  : New Relic account ID
  - api_key     : New Relic query API key
  - nrql        : NRQL query whose first result row's first numeric column is compared
  - threshold   : (optional, default 1) minimum value to be considered healthy
"""

from __future__ import annotations

import logging
from typing import Any

import requests

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

log = logging.getLogger(__name__)

NRQL_ENDPOINT = "https://insights-api.newrelic.com/v1/accounts/{account_id}/query"


class NewRelicBackend(BaseBackend):
    """Query a NRQL expression and threshold-check the result."""

    def __init__(self, config: dict[str, Any]) -> None:
        self.timeout: int = int(config.get("timeout", 10))

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        extra = pipeline.extra or {}
        account_id = extra.get("account_id")
        api_key = extra.get("api_key")
        nrql = extra.get("nrql")
        threshold = float(extra.get("threshold", 1))

        if not all([account_id, api_key, nrql]):
            log.warning("%s: missing account_id, api_key, or nrql", pipeline.name)
            return PipelineResult(pipeline.name, PipelineStatus.UNKNOWN)

        url = NRQL_ENDPOINT.format(account_id=account_id)
        headers = {"X-Query-Key": api_key, "Accept": "application/json"}
        params = {"nrql": nrql}

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=self.timeout)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            log.error("%s: New Relic request failed: %s", pipeline.name, exc)
            return PipelineResult(pipeline.name, PipelineStatus.UNKNOWN)

        try:
            results = data["results"]
            value = float(next(iter(results[0].values())))
        except (KeyError, IndexError, TypeError, ValueError) as exc:
            log.error("%s: unexpected New Relic response: %s", pipeline.name, exc)
            return PipelineResult(pipeline.name, PipelineStatus.UNKNOWN)

        status = PipelineStatus.HEALTHY if value >= threshold else PipelineStatus.FAILED
        return PipelineResult(pipeline.name, status, details={"value": value, "threshold": threshold})
