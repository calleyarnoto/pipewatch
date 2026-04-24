"""Verify that the Cassandra backend is registered in the backend registry."""

from __future__ import annotations

from pipewatch.backends import get_backend_class
from pipewatch.backends.cassandra import CassandraBackend


def test_cassandra_backend_is_registered():
    cls = get_backend_class("cassandra")
    assert cls is CassandraBackend


def test_cassandra_backend_name_case_insensitive():
    assert get_backend_class("Cassandra") is CassandraBackend
    assert get_backend_class("CASSANDRA") is CassandraBackend
