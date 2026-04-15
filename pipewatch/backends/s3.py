"""S3 backend: checks pipeline health by querying object counts or sizes in an S3 prefix."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class S3Backend(BaseBackend):
    """Check pipeline health by inspecting objects in an S3 bucket prefix.

    Pipeline config extras:
        bucket (str): S3 bucket name.
        prefix (str): Key prefix to list objects under.
        threshold (int): Minimum number of objects expected (default: 1).
        max_age_hours (float|None): If set, at least one object must have been
            modified within this many hours.
    """

    def __init__(self, **kwargs: Any) -> None:
        import boto3  # type: ignore

        region = kwargs.get("region") or kwargs.get("aws_region")
        self._s3 = boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=kwargs.get("aws_access_key_id"),
            aws_secret_access_key=kwargs.get("aws_secret_access_key"),
        )

    def check_pipeline(self, pipeline) -> PipelineResult:
        bucket: str = pipeline.extras.get("bucket", "")
        prefix: str = pipeline.extras.get("prefix", "")
        threshold: int = int(pipeline.extras.get("threshold", 1))
        max_age_hours = pipeline.extras.get("max_age_hours")

        if not bucket:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'bucket' is required in pipeline extras",
            )

        try:
            paginator = self._s3.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=bucket, Prefix=prefix)
            objects = [obj for page in pages for obj in page.get("Contents", [])]
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"S3 error: {exc}",
            )

        count = len(objects)
        if count < threshold:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.FAILED,
                message=f"Found {count} object(s) under s3://{bucket}/{prefix}, expected >= {threshold}",
            )

        if max_age_hours is not None:
            cutoff = datetime.now(tz=timezone.utc) - timedelta(hours=float(max_age_hours))
            recent = [o for o in objects if o["LastModified"] >= cutoff]
            if not recent:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.FAILED,
                    message=(
                        f"No objects modified within {max_age_hours}h under "
                        f"s3://{bucket}/{prefix}"
                    ),
                )

        return PipelineResult(
            pipeline_name=pipeline.name,
            status=PipelineStatus.HEALTHY,
            message=f"Found {count} object(s) under s3://{bucket}/{prefix}",
        )
