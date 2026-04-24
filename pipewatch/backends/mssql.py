"""Microsoft SQL Server backend for pipewatch."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class MSSQLBackend(BaseBackend):
    """Check pipeline health by running a query against a Microsoft SQL Server database.

    Config keys (under ``backend_config``):
        host (str): SQL Server hostname. Default ``"localhost"``.
        port (int): SQL Server port. Default ``1433``.
        database (str): Target database name. Default ``"master"``.
        user (str): Login username. Default ``"sa"``.
        password (str): Login password. Default ``""``.
        driver (str): ODBC driver string. Default ``"ODBC Driver 17 for SQL Server"``.

    Config keys (per pipeline ``params``):
        query (str): SQL query whose first column of the first row is the metric value.
        threshold (int | float): Minimum value considered healthy. Default ``1``.
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self._host = config.get("host", "localhost")
        self._port = int(config.get("port", 1433))
        self._database = config.get("database", "master")
        self._user = config.get("user", "sa")
        self._password = config.get("password", "")
        self._driver = config.get("driver", "ODBC Driver 17 for SQL Server")

    def check_pipeline(self, pipeline) -> PipelineResult:
        query: str = pipeline.params.get("query", "")
        threshold: float = float(pipeline.params.get("threshold", 1))

        if not query:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No query specified in pipeline params",
            )

        try:
            import pyodbc  # type: ignore

            conn_str = (
                f"DRIVER={{{self._driver}}};"
                f"SERVER={self._host},{self._port};"
                f"DATABASE={self._database};"
                f"UID={self._user};"
                f"PWD={self._password}"
            )
            conn = pyodbc.connect(conn_str, timeout=10)
            try:
                cursor = conn.cursor()
                cursor.execute(query)
                row = cursor.fetchone()
            finally:
                conn.close()
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"MSSQL error: {exc}",
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
                message=f"Non-numeric result: {value!r}",
            )

        if numeric >= threshold:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.HEALTHY,
                message=f"Value {numeric} meets threshold {threshold}",
            )
        return PipelineResult(
            pipeline_name=pipeline.name,
            status=PipelineStatus.FAILED,
            message=f"Value {numeric} below threshold {threshold}",
        )
