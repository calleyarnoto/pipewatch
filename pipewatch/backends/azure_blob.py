"""Azure Blob Storage backend for pipewatch.

Checks the number of blobs in a container (optionally filtered by prefix)
against a configurable threshold.

Required pipeline config keys:
  - connection_string: Azure Storage connection string
  - container: name of the blob container

Optional pipeline config keys:
  - prefix: blob name prefix filter (default: "")
  - threshold: minimum blob count to be considered healthy (default: 1)
"""

from __future__ import annotations

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class AzureBlobBackend(BaseBackend):
    """Backend that inspects an Azure Blob Storage container."""

    def __init__(self, config: dict) -> None:
        self._connection_string: str = config.get("connection_string", "")

    def check_pipeline(self, pipeline) -> PipelineResult:
        """Return a PipelineResult based on blob count in the container."""
        try:
            from azure.storage.blob import BlobServiceClient  # type: ignore
        except ImportError:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="azure-storage-blob package is not installed",
            )

        container: str = pipeline.config.get("container", "")
        prefix: str = pipeline.config.get("prefix", "")
        threshold: int = int(pipeline.config.get("threshold", 1))

        if not container:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'container' is required in pipeline config",
            )

        try:
            client = BlobServiceClient.from_connection_string(self._connection_string)
            container_client = client.get_container_client(container)
            blobs = list(container_client.list_blobs(name_starts_with=prefix or None))
            count = len(blobs)
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Azure Blob error: {exc}",
            )

        if count >= threshold:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.HEALTHY,
                message=f"{count} blob(s) found (threshold={threshold})",
            )

        return PipelineResult(
            name=pipeline.name,
            status=PipelineStatus.FAILED,
            message=f"only {count} blob(s) found, expected >= {threshold}",
        )
