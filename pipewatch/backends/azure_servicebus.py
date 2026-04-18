"""Azure Service Bus backend for pipewatch."""
from __future__ import annotations

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class AzureServiceBusBackend(BaseBackend):
    """Check pipeline health by inspecting Azure Service Bus queue depth."""

    def __init__(self, config: dict) -> None:
        self._conn_str: str = config.get("connection_string", "")
        self._namespace: str = config.get("namespace", "")

    def check_pipeline(self, pipeline) -> PipelineResult:
        queue_name: str = pipeline.options.get("queue_name", "")
        if not queue_name:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="queue_name not specified in pipeline options",
            )

        threshold: int = int(pipeline.options.get("threshold", 0))

        try:
            from azuremanagement import ServiceBusAdministrationClient  # type: ignore

            creds = self._conn_str or Nonereds:
                client = ServiceBusAdministrationClient.from_connection_string(creds)
            else:
                from azure.identity import DefaultAzureCredential  # type: ignore

                client = ServiceBusAdministrationClient(
                    self._namespace, DefaultAzureCredential()
                )

            props = client.get_queue_runtime_properties(queue_name)
            depth: int = props.active_message_count

            if depth <= threshold:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.HEALTHY,
                    message=f"active_message_count={depth} within threshold={threshold}",
                )
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.FAILED,
                message=f"active_message_count={depth} exceeds threshold={threshold}",
            )
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"error checking Service Bus queue: {exc}",
            )
