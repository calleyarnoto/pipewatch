"""Tests for pipewatch.backends.grpc.GRPCBackend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.grpc import GRPCBackend, _SERVING
from pipewatch.backends.base import PipelineStatus
from pipewatch.config import PipelineConfig


@pytest.fixture()
def backend() -> GRPCBackend:
    return GRPCBackend({"host": "localhost", "port": 50051, "timeout": 5.0})


@pytest.fixture()
def _pipeline() -> PipelineConfig:
    return PipelineConfig(
        name="orders-pipeline",
        extras={"service": "orders"},
    )


def _make_grpc_mocks(status_code: int):
    """Return a context-manager-compatible patch bundle for grpc imports."""
    health_pb2 = MagicMock()
    health_pb2.HealthCheckRequest.return_value = MagicMock()
    health_pb2_grpc = MagicMock()

    response = SimpleNamespace(status=status_code)
    stub_instance = MagicMock()
    stub_instance.Check.return_value = response
    health_pb2_grpc.HealthStub.return_value = stub_instance

    grpc_mod = MagicMock()
    channel_mock = MagicMock()
    grpc_mod.insecure_channel.return_value = channel_mock

    return grpc_mod, health_pb2, health_pb2_grpc


def test_healthy_when_serving(backend, _pipeline):
    grpc_mod, health_pb2, health_pb2_grpc = _make_grpc_mocks(_SERVING)
    with patch.dict(
        "sys.modules",
        {"grpc": grpc_mod, "grpc_health.v1": MagicMock(),
         "grpc_health.v1.health_pb2": health_pb2,
         "grpc_health.v1.health_pb2_grpc": health_pb2_grpc},
    ):
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.HEALTHY
    assert "SERVING" in result.message


def test_failed_when_not_serving(backend, _pipeline):
    grpc_mod, health_pb2, health_pb2_grpc = _make_grpc_mocks(2)  # NOT_SERVING
    with patch.dict(
        "sys.modules",
        {"grpc": grpc_mod, "grpc_health.v1": MagicMock(),
         "grpc_health.v1.health_pb2": health_pb2,
         "grpc_health.v1.health_pb2_grpc": health_pb2_grpc},
    ):
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.FAILED
    assert "status=2" in result.message


def test_unknown_on_rpc_error(backend, _pipeline):
    grpc_mod = MagicMock()
    grpc_mod.insecure_channel.side_effect = Exception("connection refused")
    health_pb2 = MagicMock()
    health_pb2_grpc = MagicMock()
    with patch.dict(
        "sys.modules",
        {"grpc": grpc_mod, "grpc_health.v1": MagicMock(),
         "grpc_health.v1.health_pb2": health_pb2,
         "grpc_health.v1.health_pb2_grpc": health_pb2_grpc},
    ):
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_pipeline_name_in_result(backend, _pipeline):
    grpc_mod, health_pb2, health_pb2_grpc = _make_grpc_mocks(_SERVING)
    with patch.dict(
        "sys.modules",
        {"grpc": grpc_mod, "grpc_health.v1": MagicMock(),
         "grpc_health.v1.health_pb2": health_pb2,
         "grpc_health.v1.health_pb2_grpc": health_pb2_grpc},
    ):
        result = backend.check_pipeline(_pipeline)

    assert result.pipeline_name == "orders-pipeline"


def test_defaults_used_when_no_extras(backend):
    pipeline = PipelineConfig(name="my-svc", extras={})
    grpc_mod, health_pb2, health_pb2_grpc = _make_grpc_mocks(_SERVING)
    with patch.dict(
        "sys.modules",
        {"grpc": grpc_mod, "grpc_health.v1": MagicMock(),
         "grpc_health.v1.health_pb2": health_pb2,
         "grpc_health.v1.health_pb2_grpc": health_pb2_grpc},
    ):
        result = backend.check_pipeline(pipeline)

    # service name falls back to pipeline name
    assert "my-svc" in result.message
    assert result.status == PipelineStatus.HEALTHY
