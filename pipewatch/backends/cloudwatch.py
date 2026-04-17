"""AWS CloudWatch backend for pipewatch."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class CloudWatchBackend(BaseBackend):
    """Check pipeline health via an AWS CloudWatch metric query."""

    def __init__(self, config: dict[str, Any]) -> None:
        try:
            import boto3  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise ImportError("boto3 is required for the CloudWatch backend") from exc

        self._client = boto3.client(
            "cloudwatch",
            region_name=config.get("region"),
            aws_access_key_id=config.get("aws_access_key_id"),
            aws_secret_access_key=config.get("aws_secret_access_key"),
        )

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        extra = pipeline.extra or {}
        namespace = extra.get("namespace")
        metric_name = extra.get("metric_name")
        dimension_name = extra.get("dimension_name", "PipelineName")
        period = int(extra.get("period", 300))
        threshold = float(extra.get("threshold", 1.0))
        stat = extra.get("stat", "Sum")

        if not namespace or not metric_name:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="namespace and metric_name are required in pipeline extra config",
            )

        end = datetime.now(tz=timezone.utc)
        start = end - timedelta(seconds=period * 2)

        try:
            resp = self._client.get_metric_statistics(
                Namespace=namespace,
                MetricName=metric_name,
                Dimensions=[{"Name": dimension_name, "Value": pipeline.name}],
                StartTime=start,
                EndTime=end,
                Period=period,
                Statistics=[stat],
            )
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"CloudWatch error: {exc}",
            )

        datapoints = resp.get("Datapoints", [])
        if not datapoints:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No datapoints returned from CloudWatch",
            )

        latest = sorted(datapoints, key=lambda d: d["Timestamp"])[-1]
        value = latest.get(stat, 0.0)

        if value >= threshold:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.HEALTHY,
                message=f"{metric_name}={value} meets threshold {threshold}",
            )
        return PipelineResult(
            pipeline_name=pipeline.name,
            status=PipelineStatus.FAILED,
            message=f"{metric_name}={value} below threshold {threshold}",
        )
