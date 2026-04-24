"""CockroachDB backend for pipewatch.

Checks pipeline health by executing a SQL query against a CockroachDB
database and comparing the result against a configured threshold.
Uses the psycopg2 driver (CockroachDB is PostgreSQL-wire-compatible).
"""

from __future__ import annotations

from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

_DEFAULT_THRESHOLD = 1
_DEFAULT_QUERY = "SELECT 1"


class CockroachDBBackend(BaseBackend):
    """Backend that queries a CockroachDB cluster via psycopg2."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._dsn: str = config.get("dsn", "")
        self._host: str = config.get("host", "localhost")
        self._port: int = int(config.get("port", 26257))
        self._database: str = config.get("database", "defaultdb")
        self._user: str = config.get("user", "root")
        self._password: str = config.get("password", "")
        self._sslmode: str = config.get("sslmode", "require")

    def _connect(self) -> Any:
        import psycopg2  # type: ignore[import]

        if self._dsn:
            return psycopg2.connect(self._dsn)
        return psycopg2.connect(
            host=self._host,
            port=self._port,
            dbname=self._database,
            user=self._user,
            password=self._password,
            sslmode=self._sslmode,
        )

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        query: str = pipeline.options.get("query", _DEFAULT_QUERY)
        threshold: int = int(pipeline.options.get("threshold", _DEFAULT_THRESHOLD))

        try:
            conn = self._connect()
            try:
                with conn.cursor() as cur:
                    cur.execute(query)
                    row = cur.fetchone()
            finally:
                conn.close()
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"CockroachDB error: {exc}",
            )

        if row is None:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="Query returned no rows",
            )

        value = row[0]
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Non-numeric query result: {value!r}",
            )

        if numeric >= threshold:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.HEALTHY,
                message=f"Query returned {numeric} (threshold={threshold})",
            )
        return PipelineResult(
            pipeline_name=pipeline.name,
            status=PipelineStatus.FAILED,
            message=f"Query returned {numeric} (threshold={threshold})",
        )
