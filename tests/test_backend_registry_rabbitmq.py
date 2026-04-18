"""Verify RabbitMQ backend is registered in the backend registry."""
from pipewatch.backends import get_backend_class
from pipewatch.backends.rabbitmq import RabbitMQBackend


def test_rabbitmq_backend_is_registered():
    assert get_backend_class("rabbitmq") is RabbitMQBackend


def test_rabbitmq_backend_name_case_insensitive():
    assert get_backend_class("RabbitMQ") is RabbitMQBackend
