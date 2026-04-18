"""Hive backend for pipewatch — checks row counts via HiveServer2."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class HiveBackend(BaseBackend):
    """Query HiveServer2 and evaluate a row-count against a threshold."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self.host = config.get("host", "localhost")
        self.port = int(config.get("port", 10000))
        self.username = config.get("username", "hive")
        self.database = config.get("database", "default")
        self.auth = config.get("auth", "NOSASL")

    def check_pipeline(self, pipeline) -> PipelineResult:
        query = pipeline.options.get("query")
        if not query:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No query specified in pipeline options",
            )

        threshold = int(pipeline.options.get("threshold", 1))

        try:
            from pyhive import hive  # type: ignore

            conn = hive.connect(
                host=self.host,
                port=self.port,
                username=self.username,
                database=self.database,
                auth=self.auth,
            )
            cursor = conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            conn.close()

            value = int(row[0]) if row and row[0] is not None else 0

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
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Hive error: {exc}",
            )
