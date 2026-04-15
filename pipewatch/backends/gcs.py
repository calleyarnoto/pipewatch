"""Google Cloud Storage backend for pipewatch."""
from __future__ import annotations

from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class GCSBackend(BaseBackend):
    """Check pipeline health by counting objects in a GCS bucket/prefix."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._project = config.get("project")
        self._credentials_path = config.get("credentials_path")

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        try:
            from google.cloud import storage  # type: ignore
            from google.oauth2 import service_account  # type: ignore

            extra = pipeline.extra or {}
            bucket_name: str = extra["bucket"]
            prefix: str = extra.get("prefix", "")
            threshold: int = int(extra.get("threshold", 1))

            if self._credentials_path:
                creds = service_account.Credentials.from_service_account_file(
                    self._credentials_path
                )
                client = storage.Client(project=self._project, credentials=creds)
            else:
                client = storage.Client(project=self._project)

            bucket = client.bucket(bucket_name)
            blobs = list(client.list_blobs(bucket, prefix=prefix))
            count = len(blobs)

            status = (
                PipelineStatus.HEALTHY if count >= threshold else PipelineStatus.FAILED
            )
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=status,
                message=f"Found {count} object(s) (threshold={threshold})",
            )
        except KeyError as exc:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Missing required config key: {exc}",
            )
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"GCS error: {exc}",
            )
