"""PostgreSQL backend for pipewatch — checks pipeline health via row count or custom query."""

from datetime import datetime, timezone
from typing import Optional

try:
    import psycopg2
except ImportError:  # pragma: no cover
    psycopg2 = None  # type: ignore

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class PostgresBackend(BaseBackend):
    """Backend that queries a PostgreSQL database to determine pipeline health.

    Config keys (per pipeline):
        dsn (str): PostgreSQL connection string.
        query (str): SQL query returning a single numeric value.
                     A value > 0 is considered HEALTHY; 0 is FAILED.
        threshold (int, optional): Minimum value to be considered healthy (default 1).
    """

    def __init__(self, config: dict) -> None:
        if psycopg2 is None:  # pragma: no cover
            raise ImportError(
                "psycopg2 is required for the postgres backend: pip install psycopg2-binary"
            )
        self.dsn: str = config["dsn"]
        self.threshold: int = int(config.get("threshold", 1))

    def check_pipeline(self, pipeline_name: str, pipeline_config: dict) -> PipelineResult:
        """Run the configured query and return a PipelineResult."""
        query: Optional[str] = pipeline_config.get("query")
        if not query:
            return PipelineResult(
                pipeline_name=pipeline_name,
                status=PipelineStatus.UNKNOWN,
                checked_at=datetime.now(timezone.utc),
                message="No query configured for pipeline",
            )

        try:
            conn = psycopg2.connect(self.dsn)
            try:
                with conn.cursor() as cur:
                    cur.execute(query)
                    row = cur.fetchone()
            finally:
                conn.close()
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                pipeline_name=pipeline_name,
                status=PipelineStatus.UNKNOWN,
                checked_at=datetime.now(timezone.utc),
                message=f"Query error: {exc}",
            )

        if row is None or row[0] is None:
            status = PipelineStatus.UNKNOWN
            message = "Query returned no rows"
        else:
            value = float(row[0])
            if value >= self.threshold:
                status = PipelineStatus.HEALTHY
                message = f"Query returned {value} (threshold={self.threshold})"
            else:
                status = PipelineStatus.FAILED
                message = f"Query returned {value} (threshold={self.threshold})"

        return PipelineResult(
            pipeline_name=pipeline_name,
            status=status,
            checked_at=datetime.now(timezone.utc),
            message=message,
        )
