"""BigQuery backend for pipewatch — checks pipeline health via row count queries."""

from __future__ import annotations

from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class BigQueryBackend(BaseBackend):
    """Check pipeline health by running a COUNT query against a BigQuery table."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._project = config.get("project", "")
        self._dataset = config.get("dataset", "")
        self._threshold = int(config.get("threshold", 1))
        self._credentials_path = config.get("credentials_path", None)

    def check_pipeline(self, pipeline_name: str, pipeline_config: dict[str, Any]) -> PipelineResult:
        """Run a COUNT(*) query on BigQuery and compare against the threshold."""
        from google.cloud import bigquery  # type: ignore[import]
        from google.oauth2 import service_account  # type: ignore[import]

        table = pipeline_config.get("table", pipeline_name)
        dataset = pipeline_config.get("dataset", self._dataset)
        project = pipeline_config.get("project", self._project)
        threshold = int(pipeline_config.get("threshold", self._threshold))
        where = pipeline_config.get("where", "")

        full_table = f"`{project}.{dataset}.{table}`"
        query = f"SELECT COUNT(*) AS cnt FROM {full_table}"
        if where:
            query += f" WHERE {where}"

        try:
            if self._credentials_path:
                creds = service_account.Credentials.from_service_account_file(
                    self._credentials_path
                )
                client = bigquery.Client(project=project, credentials=creds)
            else:
                client = bigquery.Client(project=project)

            rows = list(client.query(query).result())
            count = rows[0].cnt if rows else 0
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                pipeline_name=pipeline_name,
                status=PipelineStatus.UNKNOWN,
                message=f"BigQuery error: {exc}",
            )

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
