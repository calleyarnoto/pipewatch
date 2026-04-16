"""Verify GraphQL backend is registered in the backend registry."""
from __future__ import annotations

from pipewatch.backends import get_backend_class
from pipewatch.backends.graphql import GraphQLBackend


def test_graphql_backend_is_registered():
    cls = get_backend_class("graphql")
    assert cls is GraphQLBackend


def test_graphql_backend_name_case_insensitive():
    assert get_backend_class("GraphQL") is GraphQLBackend
    assert get_backend_class("GRAPHQL") is GraphQLBackend
