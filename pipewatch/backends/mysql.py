"""MySQL backend for pipewatch — checks pipeline health via row count queries."""

from __future__ import annotations

from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class MySQLBackend(BaseBackend):
    """Check pipeline health by querying row counts in a MySQL table."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._host = config.get("host", "localhost")
        self._port = int(config.get("port", 3306))
        self._user = config.get("user", "root")
        self._password = config.get("password", "")
        self._database = config.get("database", "")
        self._threshold = int(config.get("threshold", 1))

    def check_pipeline(self, pipeline_name: str, pipeline_config: dict[str, Any]) -> PipelineResult:
        """Run a COUNT query and compare against the configured threshold."""
        import mysql.connector  # type: ignore[import]

        table = pipeline_config.get("table", pipeline_name)
        where = pipeline_config.get("where", "")
        threshold = int(pipeline_config.get("threshold", self._threshold))

        query = f"SELECT COUNT(*) FROM {table}"
        if where:
            query += f" WHERE {where}"

        try:
            conn = mysql.connector.connect(
                host=self._host,
                port=self._port,
                user=self._user,
                password=self._password,
                database=self._database,
            )
            cursor = conn.cursor()
            cursor.execute(query)
            row = cursor.fetchone()
            cursor.close()
            conn.close()
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                pipeline_name=pipeline_name,
                status=PipelineStatus.UNKNOWN,
                message=f"MySQL error: {exc}",
            )

        count = row[0] if row else 0
        if count >= threshold:
            return PipelineResult(
                pipeline_name=pipeline_name,
                status=PipelineStatus.HEALTHY,
                message=f"Row count {count} meets threshold {threshold}",
            )
        return PipelineResult(
            pipeline_name=pipeline_name,
            status=PipelineStatus.FAILED,
            message=f"Row count {count} below threshold {threshold}",
        )
