"""Trino backend for pipewatch."""
from __future__ import annotations

from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class TrinoBackend(BaseBackend):
    """Check pipeline health by running a query against a Trino cluster."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._host = config.get("host", "localhost")
        self._port = int(config.get("port", 8080))
        self._user = config.get("user", "pipewatch")
        self._catalog = config.get("catalog", "hive")
        self._schema = config.get("schema", "default")

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        extra = pipeline.extra or {}
        query = extra.get("query")
        if not query:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No query specified in pipeline extra config",
            )
        threshold = int(extra.get("threshold", 1))
        try:
            import trino

            conn = trino.dbapi.connect(
                host=self._host,
                port=self._port,
                user=self._user,
                catalog=self._catalog,
                schema=self._schema,
            )
            cursor = conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            value = int(row[0]) if row else 0
            conn.close()
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Trino error: {exc}",
            )

        if value >= threshold:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.HEALTHY,
                message=f"Query returned {value} (threshold={threshold})",
            )
        return PipelineResult(
            pipeline_name=pipeline.name,
            status=PipelineStatus.FAILED,
            message=f"Query returned {value}, below threshold {threshold}",
        )
