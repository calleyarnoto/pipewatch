"""NATS JetStream backend for pipewatch.

Checks consumer lag on a NATS JetStream stream/consumer pair.
Requires the `nats-py` package.

Config keys (extras):
  stream       - JetStream stream name (required)
  consumer     - durable consumer name (required)
  threshold    - max allowed pending messages (default: 0 = any pending = unhealthy)
  servers      - comma-separated NATS server URLs (default: nats://localhost:4222)
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class NATSBackend(BaseBackend):
    """Monitor NATS JetStream consumer lag."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self._servers = config.get("servers", "nats://localhost:4222")

    def check_pipeline(self, pipeline) -> PipelineResult:
        stream = (pipeline.extras or {}).get("stream")
        consumer = (pipeline.extras or {}).get("consumer")

        if not stream or not consumer:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'stream' and 'consumer' are required in extras",
            )

        threshold = int((pipeline.extras or {}).get("threshold", 0))

        try:
            pending = asyncio.run(self._get_pending(stream, consumer))
        except Exception as exc:  # pragma: no cover
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"NATS error: {exc}",
            )

        if pending <= threshold:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.HEALTHY,
                message=f"pending={pending} within threshold={threshold}",
            )
        return PipelineResult(
            pipeline_name=pipeline.name,
            status=PipelineStatus.FAILED,
            message=f"pending={pending} exceeds threshold={threshold}",
        )

    async def _get_pending(self, stream: str, consumer: str) -> int:  # pragma: no cover
        import nats

        servers = [s.strip() for s in self._servers.split(",")]
        nc = await nats.connect(servers=servers)
        try:
            js = nc.jetstream()
            info = await js.consumer_info(stream, consumer)
            return info.num_pending
        finally:
            await nc.drain()
