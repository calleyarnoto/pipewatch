"""Tests for the Azure Service Bus backend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.azure_servicebus import AzureServiceBusBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend() -> AzureServiceBusBackend:
    return AzureServiceBusBackend({"connection_string": "Endpoint=sb://fake"})


@pytest.fixture()
def _pipeline(request):
    depth = getattr(request, "param", 0)
    return SimpleNamespace(
        name="sb-pipeline",
        options={"queue_name": "my-queue", "threshold": str(depth)},
    )


def _make_client_mock(active_count: int) -> MagicMock:
    props = MagicMock(active_message_count=active_count)
    client = MagicMock()
    client.get_queue_runtime_properties.return_value = props
    mgmt_mod = MagicMock()
    mgmt_mod.ServiceBusAdministrationClient.from_connection_string.return_value = client
    return mgmt_mod


def test_healthy_when_depth_within_threshold(backend):
    pipeline = SimpleNamespace(
        name="sb-pipeline",
        options={"queue_name": "my-queue", "threshold": "10"},
    )
    mgmt_mod = _make_client_mock(active_count=5)
    with patch.dict("sys.modules", {"azure.servicebus.management": mgmt_mod}):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.HEALTHY
    assert "5" in result.message


def test_failed_when_depth_exceeds_threshold(backend):
    pipeline = SimpleNamespace(
        name="sb-pipeline",
        options={"queue_name": "my-queue", "threshold": "3"},
    )
    mgmt_mod = _make_client_mock(active_count=20)
    with patch.dict("sys.modules", {"azure.servicebus.management": mgmt_mod}):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.FAILED
    assert "20" in result.message


def test_missing_queue_name_returns_unknown(backend):
    pipeline = SimpleNamespace(name="sb-pipeline", options={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "queue_name" in result.message


def test_unknown_on_exception(backend):
    pipeline = SimpleNamespace(
        name="sb-pipeline",
        options={"queue_name": "my-queue", "threshold": "0"},
    )
    mgmt_mod = MagicMock()
    mgmt_mod.ServiceBusAdministrationClient.from_connection_string.side_effect = RuntimeError("boom")
    with patch.dict("sys.modules", {"azure.servicebus.management": mgmt_mod}):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "boom" in result.message
