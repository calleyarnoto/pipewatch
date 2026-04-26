"""Tests for the Spark History Server backend."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.spark import SparkBackend
from pipewatch.backends.base import PipelineStatus
from pipewatch.config import PipelineConfig


@pytest.fixture()
def backend() -> SparkBackend:
    return SparkBackend({"history_server": "http://spark-history:18080", "timeout": 5})


@pytest.fixture()
def _pipeline() -> PipelineConfig:
    return PipelineConfig(
        name="my_etl_job",
        extras={"app_name": "my_etl_job"},
    )


def _mock_response(payload):
    mock = MagicMock()
    mock.json.return_value = payload
    mock.raise_for_status.return_value = None
    return mock


def test_healthy_when_last_attempt_completed_with_duration(backend, _pipeline):
    payload = [
        {
            "name": "my_etl_job",
            "attempts": [{"completed": True, "duration": 12000}],
        }
    ]
    with patch("pipewatch.backends.spark.requests.get", return_value=_mock_response(payload)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
    assert result.pipeline_name == "my_etl_job"


def test_failed_when_last_attempt_completed_with_zero_duration(backend, _pipeline):
    payload = [
        {
            "name": "my_etl_job",
            "attempts": [{"completed": True, "duration": 0}],
        }
    ]
    with patch("pipewatch.backends.spark.requests.get", return_value=_mock_response(payload)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_unknown_when_last_attempt_not_completed(backend, _pipeline):
    payload = [
        {
            "name": "my_etl_job",
            "attempts": [{"completed": False, "duration": 0}],
        }
    ]
    with patch("pipewatch.backends.spark.requests.get", return_value=_mock_response(payload)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_unknown_when_no_app_found(backend, _pipeline):
    payload = [{"name": "other_job", "attempts": [{"completed": True, "duration": 5000}]}]
    with patch("pipewatch.backends.spark.requests.get", return_value=_mock_response(payload)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_unknown_when_no_attempts(backend, _pipeline):
    payload = [{"name": "my_etl_job", "attempts": []}]
    with patch("pipewatch.backends.spark.requests.get", return_value=_mock_response(payload)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_unknown_on_request_exception(backend, _pipeline):
    with patch("pipewatch.backends.spark.requests.get", side_effect=Exception("timeout")):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_app_name_defaults_to_pipeline_name(backend):
    pipeline = PipelineConfig(name="fallback_job", extras={})
    payload = [
        {
            "name": "fallback_job",
            "attempts": [{"completed": True, "duration": 3000}],
        }
    ]
    with patch("pipewatch.backends.spark.requests.get", return_value=_mock_response(payload)):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.HEALTHY
