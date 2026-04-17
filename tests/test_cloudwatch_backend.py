"""Tests for the CloudWatch backend."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.base import PipelineStatus
from pipewatch.backends.cloudwatch import CloudWatchBackend


@pytest.fixture()
def backend():
    with patch("pipewatch.backends.cloudwatch.boto3") as mock_boto3:
        mock_boto3.client.return_value = MagicMock()
        b = CloudWatchBackend(config={"region": "us-east-1"})
        b._client = mock_boto3.client.return_value
        yield b


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(
        name="orders_etl",
        extra={
            "namespace": "MyApp",
            "metric_name": "RecordsProcessed",
            "threshold": 1,
        },
    )


def _make_response(value: float):
    return {
        "Datapoints": [
            {"Timestamp": datetime(2024, 1, 1, tzinfo=timezone.utc), "Sum": value}
        ]
    }


def test_healthy_when_value_meets_threshold(backend, _pipeline):
    backend._client.get_metric_statistics.return_value = _make_response(5.0)
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_failed_when_value_below_threshold(backend, _pipeline):
    backend._client.get_metric_statistics.return_value = _make_response(0.0)
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_unknown_when_no_datapoints(backend, _pipeline):
    backend._client.get_metric_statistics.return_value = {"Datapoints": []}
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "No datapoints" in result.message


def test_unknown_on_exception(backend, _pipeline):
    backend._client.get_metric_statistics.side_effect = RuntimeError("timeout")
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "timeout" in result.message


def test_unknown_when_missing_required_extra(backend):
    pipeline = SimpleNamespace(name="pipe", extra={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "namespace" in result.message


def test_uses_latest_datapoint(backend, _pipeline):
    backend._client.get_metric_statistics.return_value = {
        "Datapoints": [
            {"Timestamp": datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc), "Sum": 0.0},
            {"Timestamp": datetime(2024, 1, 1, 0, 5, tzinfo=timezone.utc), "Sum": 10.0},
        ]
    }
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
