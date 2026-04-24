"""Tests for the NATS JetStream backend."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pipewatch.backends.nats import NATSBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend():
    return NATSBackend({"servers": "nats://localhost:4222"})


def _pipeline(extras=None):
    return SimpleNamespace(
        name="nats-pipeline",
        extras=extras or {},
    )


def _patch_pending(pending: int):
    """Patch NATSBackend._get_pending to return *pending* without real I/O."""
    return patch.object(
        NATSBackend,
        "_get_pending",
        new_callable=lambda: lambda self: (lambda *a, **kw: None),  # replaced below
    )


def _run_with_pending(backend, pipeline, pending: int):
    """Helper: run check_pipeline with _get_pending stubbed to return *pending*."""
    import asyncio

    async def _fake(*_args):
        return pending

    with patch.object(NATSBackend, "_get_pending", _fake):
        return backend.check_pipeline(pipeline)


# ---------------------------------------------------------------------------
# Missing config
# ---------------------------------------------------------------------------

def test_missing_stream_returns_unknown(backend):
    result = backend.check_pipeline(_pipeline({"consumer": "my-consumer"}))
    assert result.status == PipelineStatus.UNKNOWN
    assert "stream" in result.message


def test_missing_consumer_returns_unknown(backend):
    result = backend.check_pipeline(_pipeline({"stream": "my-stream"}))
    assert result.status == PipelineStatus.UNKNOWN
    assert "consumer" in result.message


def test_missing_both_returns_unknown(backend):
    result = backend.check_pipeline(_pipeline({}))
    assert result.status == PipelineStatus.UNKNOWN


# ---------------------------------------------------------------------------
# Healthy / failed
# ---------------------------------------------------------------------------

def test_healthy_when_lag_within_threshold(backend):
    p = _pipeline({"stream": "events", "consumer": "etl", "threshold": "5"})
    result = _run_with_pending(backend, p, pending=3)
    assert result.status == PipelineStatus.HEALTHY
    assert result.pipeline_name == "nats-pipeline"


def test_healthy_when_lag_equals_threshold(backend):
    p = _pipeline({"stream": "events", "consumer": "etl", "threshold": "10"})
    result = _run_with_pending(backend, p, pending=10)
    assert result.status == PipelineStatus.HEALTHY


def test_failed_when_lag_exceeds_threshold(backend):
    p = _pipeline({"stream": "events", "consumer": "etl", "threshold": "0"})
    result = _run_with_pending(backend, p, pending=42)
    assert result.status == PipelineStatus.FAILED
    assert "42" in result.message


def test_default_threshold_is_zero(backend):
    """With no threshold set, any pending message should fail."""
    p = _pipeline({"stream": "events", "consumer": "etl"})
    result = _run_with_pending(backend, p, pending=1)
    assert result.status == PipelineStatus.FAILED


def test_pipeline_name_propagated(backend):
    p = _pipeline({"stream": "s", "consumer": "c"})
    result = _run_with_pending(backend, p, pending=0)
    assert result.pipeline_name == "nats-pipeline"
