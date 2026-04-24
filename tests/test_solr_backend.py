"""Tests for pipewatch.backends.solr.SolrBackend."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
import requests

from pipewatch.backends.solr import SolrBackend
from pipewatch.backends.base import PipelineStatus
from pipewatch.config import PipelineConfig


@pytest.fixture()
def backend() -> SolrBackend:
    return SolrBackend({"base_url": "http://solr:8983/solr", "timeout": 5})


@pytest.fixture()
def _pipeline() -> PipelineConfig:
    return PipelineConfig(
        name="orders",
        extras={"collection": "orders_core", "query": "status:complete", "threshold": 10},
    )


def _mock_response(num_found: int) -> MagicMock:
    resp = MagicMock()
    resp.json.return_value = {"response": {"numFound": num_found}}
    resp.raise_for_status.return_value = None
    return resp


# ---------------------------------------------------------------------------
# healthy / failed
# ---------------------------------------------------------------------------

def test_healthy_when_count_meets_threshold(backend, _pipeline):
    with patch("pipewatch.backends.solr.requests.get", return_value=_mock_response(42)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
    assert "42" in result.message


def test_failed_when_count_below_threshold(backend, _pipeline):
    with patch("pipewatch.backends.solr.requests.get", return_value=_mock_response(0)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED
    assert "0" in result.message


def test_healthy_at_exact_threshold(backend, _pipeline):
    with patch("pipewatch.backends.solr.requests.get", return_value=_mock_response(10)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY


# ---------------------------------------------------------------------------
# unknown scenarios
# ---------------------------------------------------------------------------

def test_unknown_when_collection_missing(backend):
    pipeline = PipelineConfig(name="no_coll", extras={})
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "collection" in result.message


def test_unknown_on_request_exception(backend, _pipeline):
    with patch(
        "pipewatch.backends.solr.requests.get",
        side_effect=requests.ConnectionError("refused"),
    ):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "refused" in result.message


def test_unknown_on_bad_json(backend, _pipeline):
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"unexpected": True}
    with patch("pipewatch.backends.solr.requests.get", return_value=resp):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


# ---------------------------------------------------------------------------
# registry
# ---------------------------------------------------------------------------

def test_solr_backend_is_registered():
    from pipewatch.backends import get_backend_class
    cls = get_backend_class("solr")
    assert cls is SolrBackend


def test_solr_backend_name_case_insensitive():
    from pipewatch.backends import get_backend_class
    assert get_backend_class("Solr") is SolrBackend
