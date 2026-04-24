"""Apache Solr backend — checks document count via the Solr select handler."""
from __future__ import annotations

import logging
from typing import Any

import requests

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus
from pipewatch.config import PipelineConfig

logger = logging.getLogger(__name__)


class SolrBackend(BaseBackend):
    """Query a Solr collection and compare the numFound against a threshold.

    Pipeline config extras
    ----------------------
    base_url   : str  – Solr base URL, e.g. ``http://localhost:8983/solr``
    collection : str  – collection / core name
    query      : str  – Solr query string (default ``*:*``)
    threshold  : int  – minimum numFound for a healthy result (default ``1``)
    timeout    : int  – HTTP timeout in seconds (default ``10``)
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self._base_url: str = config.get("base_url", "http://localhost:8983/solr").rstrip("/")
        self._timeout: int = int(config.get("timeout", 10))

    def check_pipeline(self, pipeline: PipelineConfig) -> PipelineResult:
        collection: str = pipeline.extras.get("collection", "")
        if not collection:
            logger.warning("[solr] pipeline '%s' missing 'collection'", pipeline.name)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'collection' is required in pipeline config",
            )

        query: str = pipeline.extras.get("query", "*:*")
        threshold: int = int(pipeline.extras.get("threshold", 1))
        url = f"{self._base_url}/{collection}/select"
        params = {"q": query, "rows": 0, "wt": "json"}

        try:
            resp = requests.get(url, params=params, timeout=self._timeout)
            resp.raise_for_status()
            data = resp.json()
            num_found: int = data["response"]["numFound"]
        except requests.RequestException as exc:
            logger.error("[solr] request failed for '%s': %s", pipeline.name, exc)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"request error: {exc}",
            )
        except (KeyError, ValueError) as exc:
            logger.error("[solr] unexpected response for '%s': %s", pipeline.name, exc)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"parse error: {exc}",
            )

        if num_found >= threshold:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.HEALTHY,
                message=f"numFound={num_found} >= threshold={threshold}",
            )
        return PipelineResult(
            pipeline_name=pipeline.name,
            status=PipelineStatus.FAILED,
            message=f"numFound={num_found} < threshold={threshold}",
        )
