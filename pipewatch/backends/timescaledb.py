"""TimescaleDB backend — checks pipeline health via a SQL query against
a TimescaleDB (PostgreSQL-compatible) database.

Config keys (under pipeline ``params``):
  - host        : DB host (default: localhost)
  - port        : DB port (default: 5432)
  - dbname      : database name (required)
  - user        : username (required)
  - password    : password (required)
  - query       : SQL query returning a single numeric value (required)
  - threshold   : minimum value considered healthy (default: 1)
"""

from __future__ import annotations

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class TimescaleDBBackend(BaseBackend):
    """Backend that runs a SQL query against TimescaleDB."""

    def __init__(self, config: dict) -> None:
        self._host = config.get("host", "localhost")
        self._port = int(config.get("port", 5432))
        self._dbname = config["dbname"]
        self._user = config["user"]
        self._password = config["password"]

    def check_pipeline(self, pipeline) -> PipelineResult:
        query: str | None = pipeline.params.get("query")
        if not query:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No query specified in pipeline params",
            )

        threshold = float(pipeline.params.get("threshold", 1))

        try:
            import psycopg2  # type: ignore

            conn = psycopg2.connect(
                host=self._host,
                port=self._port,
                dbname=self._dbname,
                user=self._user,
                password=self._password,
            )
            try:
                with conn.cursor() as cur:
                    cur.execute(query)
                    row = cur.fetchone()
            finally:
                conn.close()
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"TimescaleDB error: {exc}",
            )

        if row is None or row[0] is None:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="Query returned no rows",
            )

        value = float(row[0])
        if value >= threshold:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.HEALTHY,
                message=f"value={value} >= threshold={threshold}",
            )
        return PipelineResult(
            name=pipeline.name,
            status=PipelineStatus.FAILED,
            message=f"value={value} < threshold={threshold}",
        )
