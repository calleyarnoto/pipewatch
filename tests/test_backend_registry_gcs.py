"""Verify GCS backend is registered in the backend registry."""
from __future__ import annotations

from pipewatch.backends import get_backend_class
from pipewatch.backends.gcs import GCSBackend


def test_gcs_backend_is_registered():
    cls = get_backend_class("gcs")
    assert cls is GCSBackend


def test_gcs_backend_name_case_insensitive():
    assert get_backend_class("GCS") is GCSBackend
    assert get_backend_class("Gcs") is GCSBackend
