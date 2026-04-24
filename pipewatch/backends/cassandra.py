"""Cassandra backend for pipewatch.

Checks pipeline health by running a CQL query and comparing the result
against a configurable threshold.

Required pipeline config keys:
  - host        : Cassandra contact point (default: "localhost")
  - port        : Cassandra native transport port (default: 9042)
  - keyspace    : Keyspace to connect to
  - query       : CQL query whose first column of the first row is the metric
  - threshold   : Minimum value considered healthy (default: 1)

Optional:
  - username / password : credentials for plain-text auth
"""

from __future__ import annotations

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class CassandraBackend(BaseBackend):
    """Backend that queries an Apache Cassandra cluster via the driver."""

    def __init__(self, config: dict) -> None:
        self.contact_points = [config.get("host", "localhost")]
        self.port = int(config.get("port", 9042))
        self.username = config.get("username")
        self.password = config.get("password")

    def check_pipeline(self, pipeline) -> PipelineResult:
        try:
            from cassandra.auth import PlainTextAuthProvider
            from cassandra.cluster import Cluster
        except ImportError:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="cassandra-driver is not installed",
            )

        query = pipeline.config.get("query")
        if not query:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No query specified in pipeline config",
            )

        threshold = int(pipeline.config.get("threshold", 1))
        keyspace = pipeline.config.get("keyspace")

        auth_provider = None
        if self.username and self.password:
            auth_provider = PlainTextAuthProvider(
                username=self.username, password=self.password
            )

        try:
            cluster = Cluster(
                contact_points=self.contact_points,
                port=self.port,
                auth_provider=auth_provider,
            )
            session = cluster.connect(keyspace)
            rows = session.execute(query)
            row = rows.one()
            cluster.shutdown()

            if row is None:
                return PipelineResult(
                    name=pipeline.name,
                    status=PipelineStatus.UNKNOWN,
                    message="Query returned no rows",
                )

            value = row[0]
            status = (
                PipelineStatus.HEALTHY if value >= threshold else PipelineStatus.FAILED
            )
            return PipelineResult(
                name=pipeline.name,
                status=status,
                message=f"value={value}, threshold={threshold}",
            )
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=str(exc),
            )
