"""Backend registry for pipewatch."""
from __future__ import annotations

from typing import Type

from pipewatch.backends.base import BaseBackend

_REGISTRY: dict[str, Type[BaseBackend]] = {}


def register_backend(name: str, cls: Type[BaseBackend]) -> None:
    """Register a backend class under *name* (case-insensitive)."""
    _REGISTRY[name.lower()] = cls


def get_backend_class(name: str) -> Type[BaseBackend]:
    """Return the backend class registered under *name*.

    Raises KeyError with a helpful message when the name is unknown.
    """
    key = name.lower()
    if key not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY))
        raise KeyError(
            f"Unknown backend {name!r}. Available backends: {available}"
        )
    return _REGISTRY[key]


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
    from pipewatch.backends.kafka import KafkaBackend
    from pipewatch.backends.s3 import S3Backend
    from pipewatch.backends.dynamodb import DynamoDBBackend
    from pipewatch.backends.databricks import DatabricksBackend
    from pipewatch.backends.sftp import SFTPBackend
    from pipewatch.backends.snowflake import SnowflakeBackend
    from pipewatch.backends.gcs import GCSBackend

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
    register_backend("kafka", KafkaBackend)
    register_backend("s3", S3Backend)
    register_backend("dynamodb", DynamoDBBackend)
    register_backend("databricks", DatabricksBackend)
    register_backend("sftp", SFTPBackend)
    register_backend("snowflake", SnowflakeBackend)
    register_backend("gcs", GCSBackend)


_register_builtins()
