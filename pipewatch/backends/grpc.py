"""gRPC health-check backend.

Uses the standard gRPC Health Checking Protocol
(grpc.health.v1.Health/Check) to determine pipeline status.

Pipeline config extras
----------------------
host        : str   – gRPC server host (default: "localhost")
port        : int   – gRPC server port (default: 50051)
service     : str   – service name passed to Health/Check
                      (default: pipeline name)
timeout     : float – RPC deadline in seconds (default: 5.0)
"""
from __future__ import annotations

import logging
from typing import Any, Dict

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus
from pipewatch.config import PipelineConfig

logger = logging.getLogger(__name__)

# gRPC health proto status codes
_SERVING = 1


class GRPCBackend(BaseBackend):
    """Backend that queries a gRPC Health service."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self._default_host = config.get("host", "localhost")
        self._default_port = int(config.get("port", 50051))
        self._default_timeout = float(config.get("timeout", 5.0))

    def check_pipeline(self, pipeline: PipelineConfig) -> PipelineResult:
        try:
            import grpc
            from grpc_health.v1 import health_pb2, health_pb2_grpc
        except ImportError as exc:  # pragma: no cover
            logger.error("grpcio and grpcio-health-checking are required: %s", exc)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="grpcio packages not installed",
            )

        extras: Dict[str, Any] = pipeline.extras or {}
        host = extras.get("host", self._default_host)
        port = int(extras.get("port", self._default_port))
        service = extras.get("service", pipeline.name)
        timeout = float(extras.get("timeout", self._default_timeout))

        target = f"{host}:{port}"
        try:
            channel = grpc.insecure_channel(target)
            stub = health_pb2_grpc.HealthStub(channel)
            request = health_pb2.HealthCheckRequest(service=service)
            response = stub.Check(request, timeout=timeout)
            channel.close()
        except Exception as exc:  # noqa: BLE001
            logger.warning("gRPC health check failed for %s: %s", pipeline.name, exc)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=str(exc),
            )

        if response.status == _SERVING:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.HEALTHY,
                message=f"service '{service}' is SERVING",
            )

        return PipelineResult(
            pipeline_name=pipeline.name,
            status=PipelineStatus.FAILED,
            message=f"service '{service}' status={response.status}",
        )
