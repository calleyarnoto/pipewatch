"""Verify AzureBlobBackend is registered in the backend registry."""

from __future__ import annotations

from pipewatch.backends import get_backend_class
from pipewatch.backends.azure_blob import AzureBlobBackend


def test_azure_blob_backend_is_registered():
    cls = get_backend_class("azure_blob")
    assert cls is AzureBlobBackend


def test_azure_blob_backend_name_case_insensitive():
    assert get_backend_class("Azure_Blob") is AzureBlobBackend
    assert get_backend_class("AZURE_BLOB") is AzureBlobBackend
