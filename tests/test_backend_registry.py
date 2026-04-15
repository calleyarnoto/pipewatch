from __future__ import annotations

import pytest

from pipewatch.backends import get_backend_class, register_backend, _REGISTRY
from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class MyCustomBackend(BaseBackend):
    def __init__(self, config):
        pass

    def check_pipeline(self, pipeline):
        return PipelineResult(
            pipeline_name=pipeline.name,
            status=PipelineStatus.HEALTHY,
            message="ok",
        )


def test_builtin_backends_registered():
    for name in (
        "dummy", "airflow", "prometheus", "postgres", "mysql",
        "bigquery", "mongodb", "redis", "elasticsearch", "http",
        "snowflake", "kafka", "databricks",
    ):
        assert name in _REGISTRY, f"Expected '{name}' to be registered"


def test_get_unknown_backend_raises_key_error():
    with pytest.raises(KeyError):
        get_backend_class("nonexistent_backend")


def test_error_message_lists_available_backends():
    with pytest.raises(KeyError, match="Available backends"):
        get_backend_class("nonexistent_backend")


def test_register_custom_backend():
    register_backend("my_custom", MyCustomBackend)
    assert get_backend_class("my_custom") is MyCustomBackend
    # cleanup
    _REGISTRY.pop("my_custom", None)


def test_get_backend_class_returns_correct_class():
    from pipewatch.backends.dummy import DummyBackend
    assert get_backend_class("dummy") is DummyBackend


def test_databricks_backend_registered():
    from pipewatch.backends.databricks import DatabricksBackend
    assert get_backend_class("databricks") is DatabricksBackend
