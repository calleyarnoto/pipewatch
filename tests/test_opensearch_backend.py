"""Tests for the OpenSearch backend."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from pipewatch.backends.opensearch import OpenSearchBackend
from pipewatch.backends.base import PipelineStatus
from pipewatch.config import PipelineConfig


@pytest.fixture()
def backend() -> OpenSearchBackend:
    return OpenSearchBackend({"host": "http://opensearch:9200"})


@pytest.fixture()
def _pipeline() -> PipelineConfig:
    return PipelineConfig(
        name="orders-index",
        extras={"index": "orders-*", "threshold": 10},
    )


@pytest.fixture()
def _mock_response() -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    return resp


def test_healthy_when_count_meets_threshold(backend, _pipeline, _mock_response):
    _mock_response.json.return_value = {"count": 42}
    with patch("pipewatch.backends.opensearch.requests.post", return_value=_mock_response):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
    assert "42" in result.message


def test_failed_when_count_below_threshold(backend, _pipeline, _mock_response):
    _mock_response.json.return_value = {"count": 0}
    with patch("pipewatch.backends.opensearch.requests.post", return_value=_mock_response):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED
    assert "0" in result.message


def test_unknown_on_request_exception(backend, _pipeline):
    with patch(
        "pipewatch.backends.opensearch.requests.post",
        side_effect=requests.ConnectionError("refused"),
    ):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "refused" in result.message


def test_unknown_when_index_missing(backend):
    pipeline = PipelineConfig(name="no-index", extras={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "index" in result.message


def test_default_threshold_is_one(backend, _mock_response):
    _mock_response.json.return_value = {"count": 1}
    pipeline = PipelineConfig(name="events", extras={"index": "events"})
    with patch("pipewatch.backends.opensearch.requests.post", return_value=_mock_response):
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_auth_passed_when_configured(_mock_response):
    backend = OpenSearchBackend(
        {"host": "http://os:9200", "username": "admin", "password": "secret"}
    )
    _mock_response.json.return_value = {"count": 5}
    pipeline = PipelineConfig(name="secure", extras={"index": "secure-*"})
    with patch(
        "pipewatch.backends.opensearch.requests.post", return_value=_mock_response
    ) as mock_post:
        backend.check_pipeline(pipeline)
    _, kwargs = mock_post.call_args
    assert kwargs["auth"] == ("admin", "secret")
