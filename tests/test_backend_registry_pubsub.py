"""Verify the Pub/Sub backend is registered in the backend registry."""

from __future__ import annotations

from pipewatch.backends import get_backend_class
from pipewatch.backends.pubsub import PubSubBackend


def test_pubsub_backend_is_registered():
    cls = get_backend_class("pubsub")
    assert cls is PubSubBackend


def test_pubsub_backend_name_case_insensitive():
    assert get_backend_class("PubSub") is PubSubBackend
    assert get_backend_class("PUBSUB") is PubSubBackend
