"""Neo4j backend — checks pipeline health by running a Cypher query."""
from __future__ import annotations

from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class Neo4jBackend(BaseBackend):
    """Query a Neo4j graph database with a Cypher statement.

    Pipeline config extras
    ----------------------
    query       : str   – Cypher query that returns a single numeric value
                         in the first column of the first row.
    threshold   : int   – Minimum value considered healthy (default: 1).
    uri         : str   – Bolt URI, e.g. ``bolt://localhost:7687``
                         (overrides constructor *uri*).
    username    : str   – Overrides constructor *username*.
    password    : str   – Overrides constructor *password*.
    """

    def __init__(self, uri: str = "bolt://localhost:7687",
                 username: str = "neo4j",
                 password: str = "neo4j",
                 **kwargs: Any) -> None:
        self._uri = uri
        self._username = username
        self._password = password

    # ------------------------------------------------------------------
    def check_pipeline(self, pipeline) -> PipelineResult:
        try:
            from neo4j import GraphDatabase  # type: ignore
        except ImportError:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="neo4j package is not installed",
            )

        extras = pipeline.extras or {}
        query: str | None = extras.get("query")
        if not query:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'query' is required in pipeline extras",
            )

        threshold: int = int(extras.get("threshold", 1))
        uri = extras.get("uri", self._uri)
        username = extras.get("username", self._username)
        password = extras.get("password", self._password)

        try:
            driver = GraphDatabase.driver(uri, auth=(username, password))
            with driver.session() as session:
                record = session.run(query).single()
            driver.close()
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Neo4j error: {exc}",
            )

        if record is None:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="Query returned no rows",
            )

        value = record[0]
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Non-numeric query result: {value!r}",
            )

        status = PipelineStatus.HEALTHY if numeric >= threshold else PipelineStatus.FAILED
        return PipelineResult(
            pipeline_name=pipeline.name,
            status=status,
            message=f"value={numeric}, threshold={threshold}",
        )
