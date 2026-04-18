"""Tests for AzureEventHubBackend."""
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.azure_eventhub import AzureEventHubBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    return AzureEventHubBackend(
        {"connection_string": "Endpoint=sb://fake.servicebus.windows.net/", "eventhub_name": "my-hub"}
    )


@pytest.fixture()
def _pipeline():
    p = MagicMock()
    p.name = "eh-pipeline"
    p.options = {"consumer_group": "$Default", "max_lag": "5"}
    return p


def _make_client_mock(lags: list[int]):
    partition_ids = [str(i) for i in range(len(lags))]
    props_list = [
        {"last_enqueued_sequence_number": 100 + lag, "last_sequence_number_received": 100}
        for lag in lags
    ]
    client = MagicMock()
    client.__enter__ = lambda s: s
    client.__exit__ = MagicMock(return_value=False)
    client.get_partition_ids.return_value = partition_ids
    client.get_partition_properties.side_effect = props_list
    return client


def test_healthy_when_lag_within_threshold(backend, _pipeline):
    mock_client = _make_client_mock([2, 1])
    with patch("pipewatch.backends.azure_eventhub.EventHubConsumerClient") as cls:
        cls.from_connection_string.return_value = mock_client
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
    assert "3" in result.message


def test_failed_when_lag_exceeds_threshold(backend, _pipeline):
    mock_client = _make_client_mock([4, 4])
    with patch("pipewatch.backends.azure_eventhub.EventHubConsumerClient") as cls:
        cls.from_connection_string.return_value = mock_client
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED
    assert "8" in result.message


def test_unknown_on_import_error(backend, _pipeline):
    with patch.dict("sys.modules", {"azure.eventhub": None}):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "not installed" in result.message


def test_unknown_on_exception(backend, _pipeline):
    with patch("pipewatch.backends.azure_eventhub.EventHubConsumerClient") as cls:
        cls.from_connection_string.side_effect = RuntimeError("auth failed")
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "auth failed" in result.message


def test_pipeline_name_preserved(backend, _pipeline):
    with patch("pipewatch.backends.azure_eventhub.EventHubConsumerClient") as cls:
        cls.from_connection_string.side_effect = RuntimeError("x")
        result = backend.check_pipeline(_pipeline)
    assert result.pipeline_name == "eh-pipeline"
