"""Tests for backend base classes and the dummy backend."""

from datetime import datetime, timezone

import pytest

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus
from pipewatch.backends.dummy import DummyBackend


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def dummy_backend() -> DummyBackend:
    return DummyBackend(
        config={
            "default_status": "ok",
            "pipelines": {
                "warn_pipe": {"status": "warning", "message": "slow"},
                "fail_pipe": {"status": "critical", "message": "boom"},
            },
        }
    )


# ---------------------------------------------------------------------------
# PipelineResult tests
# ---------------------------------------------------------------------------


def test_pipeline_result_is_healthy():
    result = PipelineResult(pipeline_name="p", status=PipelineStatus.OK)
    assert result.is_healthy is True


def test_pipeline_result_not_healthy():
    result = PipelineResult(pipeline_name="p", status=PipelineStatus.CRITICAL)
    assert result.is_healthy is False


def test_pipeline_result_str_includes_name_and_status():
    ts = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    result = PipelineResult(
        pipeline_name="my_pipe",
        status=PipelineStatus.WARNING,
        last_run=ts,
        message="slow run",
    )
    text = str(result)
    assert "my_pipe" in text
    assert "warning" in text.lower()
    assert "slow run" in text


# ---------------------------------------------------------------------------
# DummyBackend tests
# ---------------------------------------------------------------------------


def test_dummy_backend_default_status(dummy_backend: DummyBackend):
    result = dummy_backend.check_pipeline("any_pipeline")
    assert result.status == PipelineStatus.OK
    assert result.pipeline_name == "any_pipeline"


def test_dummy_backend_per_pipeline_warning(dummy_backend: DummyBackend):
    result = dummy_backend.check_pipeline("warn_pipe")
    assert result.status == PipelineStatus.WARNING
    assert result.message == "slow"


def test_dummy_backend_per_pipeline_critical(dummy_backend: DummyBackend):
    result = dummy_backend.check_pipeline("fail_pipe")
    assert result.status == PipelineStatus.CRITICAL
    assert result.is_healthy is False


def test_dummy_backend_check_all(dummy_backend: DummyBackend):
    results = dummy_backend.check_all(["any_pipeline", "warn_pipe", "fail_pipe"])
    assert len(results) == 3
    statuses = {r.pipeline_name: r.status for r in results}
    assert statuses["any_pipeline"] == PipelineStatus.OK
    assert statuses["warn_pipe"] == PipelineStatus.WARNING
    assert statuses["fail_pipe"] == PipelineStatus.CRITICAL


def test_dummy_backend_metadata(dummy_backend: DummyBackend):
    result = dummy_backend.check_pipeline("any_pipeline")
    assert result.metadata.get("backend") == "dummy"


def test_dummy_backend_invalid_status_falls_back_to_unknown():
    backend = DummyBackend(config={"default_status": "not_a_real_status"})
    result = backend.check_pipeline("p")
    assert result.status == PipelineStatus.UNKNOWN


def test_dummy_backend_is_subclass_of_base():
    assert issubclass(DummyBackend, BaseBackend)
