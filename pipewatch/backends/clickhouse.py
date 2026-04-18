from __future__ import annotations

from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class ClickHouseBackend(BaseBackend):
    """Backend that checks pipeline health via a ClickHouse SQL query."""

    def __init__(self, config: dict[str, Any]) -> None:
        try:
            import clickhouse_driver  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "clickhouse-driver is required for ClickHouseBackend. "
                "Install it with: pip install clickhouse-driver"
            ) from exc
        self._host = config.get("host", "localhost")
        self._port = int(config.get("port", 9000))
        self._database = config.get("database", "default")
        self._user = config.get("user", "default")
        self._password = config.get("password", "")

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        import clickhouse_driver

        query: str = pipeline.options.get("query", "")
        threshold: int = int(pipeline.options.get("threshold", 1))

        if not query:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No query specified in pipeline options",
            )

        try:
            client = clickhouse_driver.Client(
                host=self._host,
                port=self._port,
                database=self._database,
                user=self._user,
                password=self._password,
            )
            rows = client.execute(query)
            value = rows[0][0] if rows else 0
            healthy = int(value) >= threshold
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.HEALTHY if healthy else PipelineStatus.FAILED,
                message=f"Query returned {value} (threshold={threshold})",
            )
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"ClickHouse error: {exc}",
            )
