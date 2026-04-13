"""Tests for the backend registry."""
from __future__ import annotations

import pytest

from pipewatch.backends import get_backend_class, register_backend
from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


def test_builtin_backends_registered() -> None:
    expected = [
        "airflow",
        "bigquery",
        "dummy",
        "elasticsearch",
        "http",
        "mongodb",
        "mysql",
        "postgres",
        "prometheus",
        "redis",
        "snowflake",
    ]
    for name in expected:
        cls = get_backend_class(name)
        assert issubclass(cls, BaseBackend), f"{name} should subclass BaseBackend"


def test_get_unknown_backend_raises_key_error() -> None:
    with pytest.raises(KeyError):
        get_backend_class("nonexistent_backend")


def test_error_message_lists_available_backends() -> None:
    with pytest.raises(KeyError, match="snowflake"):
        get_backend_class("nonexistent_backend")


def test_register_custom_backend() -> None:
    class MyCustomBackend(BaseBackend):
        def __init__(self, config: dict) -> None:  # noqa: ANN001
            pass

        def check_pipeline(self, pipeline: dict) -> PipelineResult:  # noqa: ANN001
            return PipelineResult(
                name=pipeline["name"],
                status=PipelineStatus.HEALTHY,
                message="ok",
            )

    register_backend("my_custom", MyCustomBackend)
    assert get_backend_class("my_custom") is MyCustomBackend


def test_snowflake_backend_in_registry() -> None:
    from pipewatch.backends.snowflake import SnowflakeBackend

    cls = get_backend_class("snowflake")
    assert cls is SnowflakeBackend
