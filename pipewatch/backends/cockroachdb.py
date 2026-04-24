"""CockroachDB backend for pipewatch.

Checks pipeline health by running a SQL query against a CockroachDB cluster
and comparing the returned count against a configurable threshold.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)

_DEFAULT_THRESHOLD = 1
_DEFAULT_PORT = 26257


class CockroachDBBackend(BaseBackend):
    """Backend that queries a CockroachDB cluster via the psycopg2 driver."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self._host = config.get("host", "localhost")
        self._port = int(config.get("port", _DEFAULT_PORT))
        self._database = config.get("database", "defaultdb")
        self._user = config.get("user", "root")
        self._password = config.get("password", "")
        self._sslmode = config.get("sslmode", "require")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self):
        """Return a live psycopg2 connection to CockroachDB."""
        import psycopg2  # type: ignore

        return psycopg2.connect(
            host=self._host,
            port=self._port,
            dbname=self._database,
            user=self._user,
            password=self._password,
            sslmode=self._sslmode,
        )

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def check_pipeline(self, pipeline) -> PipelineResult:
        """Execute the configured query and evaluate the result."""
        query: str = pipeline.config.get("query", "")
        threshold: int = int(pipeline.config.get("threshold", _DEFAULT_THRESHOLD))

        if not query:
            logger.warning("CockroachDB backend: no query configured for '%s'", pipeline.name)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No query configured",
            )

        try:
            conn = self._connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(query)
                    row = cur.fetchone()
                    value = int(row[0]) if row else 0
            finally:
                conn.close()
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception("CockroachDB backend error for '%s': %s", pipeline.name, exc)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Error querying CockroachDB: {exc}",
            )

        if value >= threshold:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.HEALTHY,
                message=f"Count {value} meets threshold {threshold}",
            )

        return PipelineResult(
            pipeline_name=pipeline.name,
            status=PipelineStatus.FAILED,
            message=f"Count {value} below threshold {threshold}",
        )
