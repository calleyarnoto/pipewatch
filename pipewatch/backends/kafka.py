"""Kafka backend: checks consumer group lag as a pipeline health signal."""

from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class KafkaBackend(BaseBackend):
    """Check pipeline health by inspecting Kafka consumer group lag.

    Config keys (top-level backend config):
        bootstrap_servers (str): Comma-separated list of broker addresses.

    Pipeline-level extra keys:
        group_id (str):        Consumer group to inspect.
        topic (str):           Topic to measure lag on (optional; sums all topics if omitted).
        max_lag (int):         Maximum acceptable total lag. Default: 0 (no lag allowed).
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        self._bootstrap_servers = config.get("bootstrap_servers", "localhost:9092")

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        extra: Dict[str, Any] = pipeline.extra or {}
        group_id: str | None = extra.get("group_id")
        topic: str | None = extra.get("topic")
        max_lag: int = int(extra.get("max_lag", 0))

        if not group_id:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'group_id' is required in pipeline extra config",
            )

        try:
            from kafka import KafkaAdminClient, KafkaConsumer  # type: ignore
            from kafka.structs import TopicPartition  # type: ignore
        except ImportError:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="kafka-python is not installed; run: pip install kafka-python",
            )

        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self._bootstrap_servers,
                group_id=group_id,
            )
            partitions = consumer.partitions_for_topic(topic) if topic else None

            if topic and partitions is None:
                consumer.close()
                return PipelineResult(
                    name=pipeline.name,
                    status=PipelineStatus.UNKNOWN,
                    message=f"Topic '{topic}' not found",
                )

            tps = (
                [TopicPartition(topic, p) for p in partitions]
                if topic
                else list(consumer.assignment())
            )

            end_offsets = consumer.end_offsets(tps)
            committed = {tp: consumer.committed(tp) or 0 for tp in tps}
            total_lag = sum(
                max(end_offsets[tp] - committed[tp], 0) for tp in tps
            )
            consumer.close()
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Error querying Kafka: {exc}",
            )

        if total_lag <= max_lag:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.HEALTHY,
                message=f"Consumer lag {total_lag} is within threshold ({max_lag})",
            )
        return PipelineResult(
            name=pipeline.name,
            status=PipelineStatus.FAILED,
            message=f"Consumer lag {total_lag} exceeds threshold ({max_lag})",
        )
