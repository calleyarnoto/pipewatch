from __future__ import annotations

from typing import Type

from pipewatch.backends.base import BaseBackend

_REGISTRY: dict[str, Type[BaseBackend]] = {}


def register_backend(name: str, cls: Type[BaseBackend]) -> None:
    """Register a backend class under *name*."""
    _REGISTRY[name] = cls


def get_backend_class(name: str) -> Type[BaseBackend]:
    """Return the backend class for *name*, raising KeyError if unknown."""
    if name not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY))
        raise KeyError(
            f"Unknown backend '{name}'. Available backends: {available}"
        )
    return _REGISTRY[name]


def _register_builtins() -> None:
    from pipewatch.backends.dummy import DummyBackend
    from pipewatch.backends.airflow import AirflowBackend
    from pipewatch.backends.prometheus import PrometheusBackend
    from pipewatch.backends.postgres import PostgresBackend
    from pipewatch.backends.mysql import MySQLBackend
    from pipewatch.backends.bigquery import BigQueryBackend
    from pipewatch.backends.mongodb import MongoDBBackend
    from pipewatch.backends.redis import RedisBackend
    from pipewatch.backends.elasticsearch import ElasticsearchBackend
    from pipewatch.backends.http import HTTPBackend
    from pipewatch.backends.snowflake import SnowflakeBackend
    from pipewatch.backends.kafka import KafkaBackend
    from pipewatch.backends.databricks import DatabricksBackend

    register_backend("dummy", DummyBackend)
    register_backend("airflow", AirflowBackend)
    register_backend("prometheus", PrometheusBackend)
    register_backend("postgres", PostgresBackend)
    register_backend("mysql", MySQLBackend)
    register_backend("bigquery", BigQueryBackend)
    register_backend("mongodb", MongoDBBackend)
    register_backend("redis", RedisBackend)
    register_backend("elasticsearch", ElasticsearchBackend)
    register_backend("http", HTTPBackend)
    register_backend("snowflake", SnowflakeBackend)
    register_backend("kafka", KafkaBackend)
    register_backend("databricks", DatabricksBackend)


_register_builtins()
