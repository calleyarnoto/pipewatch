"""Tests for the backend registry."""

from __future__ import annotations

import pytest

from pipewatch.backends import get_backend_class, register_backend, _REGISTRY
from pipewatch.backends.dummy import DummyBackend
from pipewatch.backends.airflow import AirflowBackend
from pipewatch.backends.prometheus import PrometheusBackend
from pipewatch.backends.postgres import PostgresBackend
from pipewatch.backends.mysql import MySQLBackend
from pipewatch.backends.bigquery import BigQueryBackend
from pipewatch.backends.mongodb import MongoDBBackend
from pipewatch.backends.redis import RedisBackend
from pipewatch.backends.elasticsearch import ElasticsearchBackend


@pytest.mark.parametrize(
    "name, expected_cls",
    [
        ("dummy", DummyBackend),
        ("airflow", AirflowBackend),
        ("prometheus", PrometheusBackend),
        ("postgres", PostgresBackend),
        ("mysql", MySQLBackend),
        ("bigquery", BigQueryBackend),
        ("mongodb", MongoDBBackend),
        ("redis", RedisBackend),
        ("elasticsearch", ElasticsearchBackend),
    ],
)
def test_builtin_backends_registered(name: str, expected_cls: type) -> None:
    assert get_backend_class(name) is expected_cls


def test_get_unknown_backend_raises_key_error() -> None:
    with pytest.raises(KeyError, match="unknown_backend"):
        get_backend_class("unknown_backend")


def test_error_message_lists_available_backends() -> None:
    with pytest.raises(KeyError, match="Available"):
        get_backend_class("does_not_exist")


def test_register_custom_backend() -> None:
    from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

    class MyCustomBackend(BaseBackend):
        def check_pipeline(self, pipeline_name, pipeline_config):
            return PipelineResult(pipeline_name=pipeline_name, status=PipelineStatus.HEALTHY)

    register_backend("custom", MyCustomBackend)
    assert get_backend_class("custom") is MyCustomBackend
    # cleanup
    del _REGISTRY["custom"]


def test_registry_contains_all_builtins() -> None:
    expected = {
        "dummy", "airflow", "prometheus", "postgres",
        "mysql", "bigquery", "mongodb", "redis", "elasticsearch",
    }
    assert expected.issubset(set(_REGISTRY.keys()))
