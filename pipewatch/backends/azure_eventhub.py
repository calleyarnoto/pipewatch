"""Azure Event Hubs backend for pipewatch."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class AzureEventHubBackend(BaseBackend):
    """Check pipeline health via Azure Event Hub consumer group lag."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self._connection_str: str = config["connection_string"]
        self._eventhub_name: str = config["eventhub_name"]

    def check_pipeline(self, pipeline) -> PipelineResult:
        consumer_group = pipeline.options.get("consumer_group", "$Default")
        threshold = int(pipeline.options.get("max_lag", 0))

        try:
            from azure.eventhub import EventHubConsumerClient
        except ImportError:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="azure-eventhub package not installed",
            )

        try:
            client = EventHubConsumerClient.from_connection_string(
                self._connection_str,
                consumer_group=consumer_group,
                eventhub_name=self._eventhub_name,
            )
            with client:
                partition_ids = client.get_partition_ids()
                total_lag = 0
                for pid in partition_ids:
                    props = client.get_partition_properties(pid)
                    last_enqueued = props["last_enqueued_sequence_number"]
                    last_read = props.get("last_sequence_number_received", last_enqueued)
                    total_lag += max(0, last_enqueued - last_read)

            if total_lag <= threshold:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.HEALTHY,
                    message=f"total lag {total_lag} within threshold {threshold}",
                )
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.FAILED,
                message=f"total lag {total_lag} exceeds threshold {threshold}",
            )
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=str(exc),
            )
