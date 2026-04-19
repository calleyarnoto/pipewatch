"""Ensure TrinoBackend is registered in the backend registry."""
from pipewatch.backends import get_backend_class
from pipewatch.backends.trino import TrinoBackend


def test_trino_backend_is_registered():
    cls = get_backend_class("trino")
    assert cls is TrinoBackend


def test_trino_backend_name_case_insensitive():
    cls = get_backend_class("Trino")
    assert cls is TrinoBackend
