"""Tests for the Elasticsearch backend."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.elasticsearch import ElasticsearchBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend() -> ElasticsearchBackend:
    return ElasticsearchBackend(
        {
            "url": "http://localhost:9200",
            "index": "pipeline-events",
            "threshold": 1,
        }
    )


def _mock_response(count: int, raise_exc: Exception | None = None) -> MagicMock:
    mock = MagicMock()
    if raise_exc:
        mock.raise_for_status.side_effect = raise_exc
    else:
        mock.raise_for_status.return_value = None
        mock.json.return_value = {"count": count}
    return mock


def test_healthy_when_count_meets_threshold(backend: ElasticsearchBackend) -> None:
    with patch("requests.post", return_value=_mock_response(5)):
        result = backend.check_pipeline("my_pipeline", {})
    assert result.status == PipelineStatus.HEALTHY
    assert "count=5" in result.message


def test_failed_when_count_below_threshold(backend: ElasticsearchBackend) -> None:
    with patch("requests.post", return_value=_mock_response(0)):
        result = backend.check_pipeline("my_pipeline", {})
    assert result.status == PipelineStatus.FAILED
    assert "count=0" in result.message


def test_unknown_on_request_exception(backend: ElasticsearchBackend) -> None:
    with patch("requests.post", side_effect=ConnectionError("refused")):
        result = backend.check_pipeline("my_pipeline", {})
    assert result.status == PipelineStatus.UNKNOWN
    assert "refused" in result.message


def test_unknown_on_http_error(backend: ElasticsearchBackend) -> None:
    import requests

    with patch("requests.post", return_value=_mock_response(0, raise_exc=requests.HTTPError("500"))):
        result = backend.check_pipeline("my_pipeline", {})
    assert result.status == PipelineStatus.UNKNOWN


def test_custom_threshold_overrides_default(backend: ElasticsearchBackend) -> None:
    with patch("requests.post", return_value=_mock_response(3)):
        result = backend.check_pipeline("my_pipeline", {"threshold": 10})
    assert result.status == PipelineStatus.FAILED
    assert "threshold=10" in result.message


def test_pipeline_name_in_result(backend: ElasticsearchBackend) -> None:
    with patch("requests.post", return_value=_mock_response(1)):
        result = backend.check_pipeline("events_pipeline", {})
    assert result.pipeline_name == "events_pipeline"


def test_custom_index_in_pipeline_config(backend: ElasticsearchBackend) -> None:
    """Index specified per-pipeline should be passed to the URL."""
    captured: list[str] = []

    def fake_post(url: str, **kwargs):
        captured.append(url)
        return _mock_response(2)

    with patch("requests.post", side_effect=fake_post):
        backend.check_pipeline("my_pipeline", {"index": "custom-index"})

    assert "custom-index" in captured[0]
