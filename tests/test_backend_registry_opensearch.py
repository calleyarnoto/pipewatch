"""Verify OpenSearch backend is registered in the backend registry."""
from __future__ import annotations

from pipewatch.backends import get_backend_class
from pipewatch.backends.opensearch import OpenSearchBackend


def test_opensearch_backend_is_registered():
    cls = get_backend_class("opensearch")
    assert cls is OpenSearchBackend


def test_opensearch_backend_name_case_insensitive():
    assert get_backend_class("OpenSearch") is OpenSearchBackend
    assert get_backend_class("OPENSEARCH") is OpenSearchBackend
