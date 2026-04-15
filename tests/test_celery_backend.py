"""Tests for the Celery backend."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.base import PipelineStatus
from pipewatch.backends.celery import CeleryBackend


@pytest.fixture()
def backend():
    with patch("pipewatch.backends.celery.Celery"):
        return CeleryBackend({"broker_url": "redis://localhost:6379/0"})


@pytest.fixture()
def _pipeline():
    p = MagicMock()
    p.name = "orders_pipeline"
    p.extras = {"queue": "etl"}
    return p


def _make_inspect_mock(active_tasks):
    """Return a mock inspect object that reports *active_tasks* on the 'etl' queue."""
    task_list = [
        {"delivery_info": {"routing_key": "etl"}} for _ in range(active_tasks)
    ]
    inspect_mock = MagicMock()
    inspect_mock.active.return_value = {"worker1@host": task_list}
    return inspect_mock


def test_missing_queue_returns_unknown(backend):
    pipeline = MagicMock()
    pipeline.name = "no_queue"
    pipeline.extras = {}
    result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "queue" in result.message


def test_healthy_when_active_count_within_bounds(backend, _pipeline):
    _pipeline.extras = {"queue": "etl", "min_active": 1, "max_active": 10}
    inspect_mock = _make_inspect_mock(3)
    backend._app.control.inspect.return_value = inspect_mock
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
    assert "3" in result.message


def test_failed_when_below_min_active(backend, _pipeline):
    _pipeline.extras = {"queue": "etl", "min_active": 2}
    inspect_mock = _make_inspect_mock(0)
    backend._app.control.inspect.return_value = inspect_mock
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED
    assert "min_active" in result.message


def test_failed_when_above_max_active(backend, _pipeline):
    _pipeline.extras = {"queue": "etl", "max_active": 5}
    inspect_mock = _make_inspect_mock(8)
    backend._app.control.inspect.return_value = inspect_mock
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED
    assert "max_active" in result.message


def test_unknown_on_inspect_exception(backend, _pipeline):
    backend._app.control.inspect.side_effect = Exception("broker unreachable")
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN
    assert "broker unreachable" in result.message


def test_healthy_with_no_active_tasks_and_no_min(backend, _pipeline):
    inspect_mock = _make_inspect_mock(0)
    backend._app.control.inspect.return_value = inspect_mock
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_inspect_returns_none_treated_as_empty(backend, _pipeline):
    inspect_mock = MagicMock()
    inspect_mock.active.return_value = None
    backend._app.control.inspect.return_value = inspect_mock
    result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY
