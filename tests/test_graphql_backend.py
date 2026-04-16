"""Tests for the GraphQL backend."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
import requests

from pipewatch.backends.graphql import GraphQLBackend
from pipewatch.backends.base import PipelineStatus

BACKEND_CONFIG = {"url": "http://graphql.example.com/graphql", "timeout": 5}


@pytest.fixture()
def backend() -> GraphQLBackend:
    return GraphQLBackend(BACKEND_CONFIG)


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(
        name="orders_pipeline",
        extra={
            "query": "{ stats { row_count } }",
            "field_path": "stats.row_count",
            "threshold": 1,
        },
    )


def _mock_response(data: dict, status_code: int = 200) -> MagicMock:
    mock = MagicMock()
    mock.status_code = status_code
    mock.json.return_value = {"data": data}
    mock.raise_for_status = MagicMock()
    return mock


def test_healthy_when_value_meets_threshold(backend, _pipeline):
    with patch("pipewatch.backends.graphql.requests.post") as mock_post:
        mock_post.return_value = _mock_response({"stats": {"row_count": 42}})
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_failed_when_value_below_threshold(backend, _pipeline):
    with patch("pipewatch.backends.graphql.requests.post") as mock_post:
        mock_post.return_value = _mock_response({"stats": {"row_count": 0}})
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_unknown_when_field_path_missing(backend, _pipeline):
    with patch("pipewatch.backends.graphql.requests.post") as mock_post:
        mock_post.return_value = _mock_response({"other": {}})
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_unknown_on_request_exception(backend, _pipeline):
    with patch("pipewatch.backends.graphql.requests.post") as mock_post:
        mock_post.side_effect = requests.ConnectionError("unreachable")
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_unknown_when_query_missing(backend):
    pipeline = SimpleNamespace(
        name="no_query",
        extra={"field_path": "stats.row_count"},
    )
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_unknown_when_field_path_missing_from_config(backend):
    pipeline = SimpleNamespace(
        name="no_field",
        extra={"query": "{ stats { row_count } }"},
    )
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_custom_threshold(backend):
    pipeline = SimpleNamespace(
        name="custom_thresh",
        extra={
            "query": "{ stats { row_count } }",
            "field_path": "stats.row_count",
            "threshold": 100,
        },
    )
    with patch("pipewatch.backends.graphql.requests.post") as mock_post:
        mock_post.return_value = _mock_response({"stats": {"row_count": 50}})
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.FAILED


def test_result_carries_pipeline_name(backend, _pipeline):
    with patch("pipewatch.backends.graphql.requests.post") as mock_post:
        mock_post.return_value = _mock_response({"stats": {"row_count": 10}})
        result = backend.check_pipeline(_pipeline)
    assert result.pipeline_name == "orders_pipeline"
