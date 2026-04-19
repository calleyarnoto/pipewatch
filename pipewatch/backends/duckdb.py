"""DuckDB backend for pipewatch."""
from __future__ import annotations

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class DuckDBBackend(BaseBackend):
    """Check pipeline health by running a query against a DuckDB database."""

    def __init__(self, config: dict) -> None:
        self._database = config.get("database", ":memory:")

    def check_pipeline(self, pipeline) -> PipelineResult:
        try:
            import duckdb
        except ImportError:  # pragma: no cover
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="duckdb package is not installed",
            )

        query = pipeline.options.get("query")
        if not query:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No query configured for pipeline",
            )

        threshold = int(pipeline.options.get("threshold", 1))

        try:
            conn = duckdb.connect(self._database, read_only=True)
            try:
                rows = conn.execute(query).fetchall()
                value = rows[0][0] if rows else 0
            finally:
                conn.close()
        except Exception as exc:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"DuckDB error: {exc}",
            )

        if value is None:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="Query returned NULL",
            )

        numeric = float(value)
        if numeric >= threshold:
            status = PipelineStatus.HEALTHY
            message = f"Value {numeric} meets threshold {threshold}"
        else:
            status = PipelineStatus.FAILED
            message = f"Value {numeric} below threshold {threshold}"

        return PipelineResult(
            pipeline_name=pipeline.name,
            status=status,
            message=message,
        )
