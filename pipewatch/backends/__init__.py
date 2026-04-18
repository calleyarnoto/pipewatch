"""Backend registry — maps string keys to backend classes."""
from __future__ import annotations

from typing import Dict, Type

from pipewatch.backends.base import BaseBackend

_REGISTRY: Dict[str, Type[BaseBackend]] = {}


def register_backend(name: str, cls: Type[BaseBackend]) -> None:
    _REGISTRY[name.lower()] = cls


def get_backend_class(name: str) -> Type[BaseBackend]:
    key = name.lower()
    if key not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY))
        raise KeyError(
            f"Unknown backend '{name}'. Available backends: {available}"
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
    from pipewatch.backends.snowflake import SnowflakeBackend
    from pipewatch.backends.kafka import KafkaBackend
    from pipewatch.backends.s3 import S3Backend
    from pipewatch.backends.dynamodb import DynamoDBBackend
    from pipewatch.backends.databricks import DatabricksBackend
    from pipewatch.backends.sftp import SFTPBackend
    from pipewatch.backends.gcs import GCSBackend
    from pipewatch.backends.azure_blob import AzureBlobBackend
    from pipewatch.backends.ftp import FTPBackend
    from pipewatch.backends.celery import CeleryBackend
    from pipewatch.backends.grpc import GRPCBackend
    from pipewatch.backends.graphql import GraphQLBackend
    from pipewatch.backends.sqs import SQSBackend
    from pipewatch.backends.pubsub import PubSubBackend
    from pipewatch.backends.splunk import SplunkBackend
    from pipewatch.backends.datadog import DatadogBackend
    from pipewatch.backends.newrelic import NewRelicBackend
    from pipewatch.backends.cloudwatch import CloudWatchBackend
    from pipewatch.backends.influxdb import InfluxDBBackend
    from pipewatch.backends.azure_eventhub import AzureEventHubBackend
    from pipewatch.backends.azure_servicebus import AzureServiceBusBackend
    from pipewatch.backends.rabbitmq import RabbitMQBackend

    for cls in [
        DummyBackend, AirflowBackend, PrometheusBackend, PostgresBackend,
        MySQLBackend, BigQueryBackend, MongoDBBackend, RedisBackend,
        ElasticsearchBackend, HTTPBackend, SnowflakeBackend, KafkaBackend,
        S3Backend, DynamoDBBackend, DatabricksBackend, SFTPBackend,
        GCSBackend, AzureBlobBackend, FTPBackend, CeleryBackend,
        GRPCBackend, GraphQLBackend, SQSBackend, PubSubBackend,
        SplunkBackend, DatadogBackend, NewRelicBackend, CloudWatchBackend,
        InfluxDBBackend, AzureEventHubBackend, AzureServiceBusBackend,
        RabbitMQBackend,
    ]:
        register_backend(cls.name, cls)


_register_builtins()
