"""AWS SQS backend — checks queue depth against a threshold."""
from __future__ import annotations

import logging
from typing import Any, Dict

import boto3

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)

_DEFAULT_THRESHOLD = 1
_DEFAULT_REGION = "us-east-1"


class SQSBackend(BaseBackend):
    """Backend that measures the approximate number of messages in an SQS queue."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self._region = config.get("region", _DEFAULT_REGION)
        aws_access_key = config.get("aws_access_key_id")
        aws_secret_key = config.get("aws_secret_access_key")

        session_kwargs: Dict[str, Any] = {"region_name": self._region}
        if aws_access_key and aws_secret_key:
            session_kwargs["aws_access_key_id"] = aws_access_key
            session_kwargs["aws_secret_access_key"] = aws_secret_key

        session = boto3.Session(**session_kwargs)
        self._client = session.client("sqs")

    def check_pipeline(self, pipeline) -> PipelineResult:
        queue_url: str | None = pipeline.config.get("queue_url")
        if not queue_url:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'queue_url' is required in pipeline config",
            )

        threshold: int = int(pipeline.config.get("threshold", _DEFAULT_THRESHOLD))

        try:
            response = self._client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["ApproximateNumberOfMessages"],
            )
            count = int(
                response["Attributes"].get("ApproximateNumberOfMessages", 0)
            )
        except Exception as exc:  # noqa: BLE001
            logger.exception("SQS request failed for queue %s", queue_url)
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"SQS error: {exc}",
            )

        if count >= threshold:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.HEALTHY,
                message=f"Queue depth {count} meets threshold {threshold}",
            )
        return PipelineResult(
            name=pipeline.name,
            status=PipelineStatus.FAILED,
            message=f"Queue depth {count} below threshold {threshold}",
        )
