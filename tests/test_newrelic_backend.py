import pytest
import requests

from unittest.mock import MagicMock, patch
from pipewatch.backends.newrelic import NewRelicBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    return NewRelicBackend({})


@pytest.fixture()
def _pipeline():
    p = MagicMock()
    p.name = "my_pipeline"
    p.extra = {
        "account_id": "123456",
        "api_key": "NRAK-TESTKEY",
        "nrql": "SELECT count(*) FROM MyEvent SINCE 1 hour ago",
    }
    return p


def _mock_response(json_data, status_code=200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data
    resp.raise_for_status = MagicMock()
    return resp


def test_healthy_when_value_meets_threshold(backend, _pipeline):
    payload = {"results": [{"count": 42}]}
    with patch("pipewatch.backends.newrelic.requests.get", return_value=_mock_response(payload)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
    assert result.details["value"] == 42.0


def test_failed_when_value_below_threshold(backend, _pipeline):
    _pipeline.extra["threshold"] = 10
    payload = {"results": [{"count": 0}]}
    with patch("pipewatch.backends.newrelic.requests.get", return_value=_mock_response(payload)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_custom_threshold_respected(backend, _pipeline):
    _pipeline.extra["threshold"] = 5
    payload = {"results": [{"count": 5}]}
    with patch("pipewatch.backends.newrelic.requests.get", return_value=_mock_response(payload)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_unknown_on_request_exception(backend, _pipeline):
    with patch("pipewatch.backends.newrelic.requests.get", side_effect=requests.RequestException("timeout")):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_unknown_on_missing_config(backend, _pipeline):
    _pipeline.extra = {}
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_unknown_on_bad_response_shape(backend, _pipeline):
    payload = {"results": []}
    with patch("pipewatch.backends.newrelic.requests.get", return_value=_mock_response(payload)):
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_newrelic_backend_registered():
    from pipewatch.backends import get_backend_class
    cls = get_backend_class("newrelic")
    assert cls is NewRelicBackend
