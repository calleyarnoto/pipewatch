"""DynamoDB backend for pipewatch.

Checks pipeline health by scanning or querying a DynamoDB table
and comparing the item count against a configurable threshold.
"""
from __future__ import annotations

from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class DynamoDBBackend(BaseBackend):
    """Backend that queries an AWS DynamoDB table."""

    def __init__(self, config: dict[str, Any]) -> None:
        import boto3  # type: ignore

        self._client = boto3.client(
            "dynamodb",
            region_name=config.get("region", "us-east-1"),
            aws_access_key_id=config.get("aws_access_key_id"),
            aws_secret_access_key=config.get("aws_secret_access_key"),
        )

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        """Return a PipelineResult based on item count in a DynamoDB table.

        Pipeline config keys:
            table      (str)  – DynamoDB table name (required)
            index      (str)  – Optional GSI/LSI name to query
            threshold  (int)  – Minimum item count to be considered healthy (default 1)
        """
        table: str | None = pipeline.config.get("table")
        if not table:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'table' is required in pipeline config",
            )

        threshold: int = int(pipeline.config.get("threshold", 1))

        try:
            kwargs: dict[str, Any] = {"TableName": table, "Select": "COUNT"}
            index = pipeline.config.get("index")
            if index:
                kwargs["IndexName"] = index

            count: int = self._get_total_count(kwargs)

            if count >= threshold:
                return PipelineResult(
                    name=pipeline.name,
                    status=PipelineStatus.HEALTHY,
                    message=f"item count {count} meets threshold {threshold}",
                )
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.FAILED,
                message=f"item count {count} below threshold {threshold}",
            )
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"DynamoDB error: {exc}",
            )

    def _get_total_count(self, kwargs: dict[str, Any]) -> int:
        """Scan the table and accumulate the count across all paginated responses.

        DynamoDB paginates scan results when the result set exceeds 1 MB.
        Summing ``Count`` from every page ensures the returned value reflects
        all matching items, not just the first page.
        """
        total = 0
        while True:
            response = self._client.scan(**kwargs)
            total += response.get("Count", 0)
            last_key = response.get("LastEvaluatedKey")
            if not last_key:
                break
            kwargs["ExclusiveStartKey"] = last_key
        return total
