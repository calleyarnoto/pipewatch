"""Snowflake backend for pipewatch."""
from __future__ import annotations

from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class SnowflakeBackend(BaseBackend):
    """Check pipeline health by querying row counts in Snowflake."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._account = config["account"]
        self._user = config["user"]
        self._password = config["password"]
        self._database = config.get("database", "")
        self._schema = config.get("schema", "PUBLIC")
        self._warehouse = config.get("warehouse", "")
        self._role = config.get("role", "")

    def check_pipeline(self, pipeline: dict[str, Any]) -> PipelineResult:
        """Run a SQL query against Snowflake and evaluate the result."""
        import snowflake.connector  # type: ignore[import]

        name: str = pipeline["name"]
        query: str = pipeline["query"]
        threshold: int = int(pipeline.get("threshold", 1))

        connect_kwargs: dict[str, Any] = {
            "account": self._account,
            "user": self._user,
            "password": self._password,
        }
        if self._database:
            connect_kwargs["database"] = self._database
        if self._schema:
            connect_kwargs["schema"] = self._schema
        if self._warehouse:
            connect_kwargs["warehouse"] = self._warehouse
        if self._role:
            connect_kwargs["role"] = self._role

        try:
            conn = snowflake.connector.connect(**connect_kwargs)
            try:
                cur = conn.cursor()
                cur.execute(query)
                row = cur.fetchone()
                count = int(row[0]) if row else 0
            finally:
                conn.close()
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                name=name,
                status=PipelineStatus.UNKNOWN,
                message=f"Snowflake error: {exc}",
            )

        if count >= threshold:
            return PipelineResult(
                name=name,
                status=PipelineStatus.HEALTHY,
                message=f"Row count {count} meets threshold {threshold}",
            )
        return PipelineResult(
            name=name,
            status=PipelineStatus.FAILED,
            message=f"Row count {count} below threshold {threshold}",
        )
