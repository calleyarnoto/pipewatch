"""Verify the Datadog backend is registered in the backend registry."""
from pipewatch.backends import get_backend_class
from pipewatch.backends.datadog import DatadogBackend


def test_datadog_backend_is_registered():
    cls = get_backend_class("datadog")
    assert cls is DatadogBackend


def test_datadog_backend_name_case_insensitive():
    assert get_backend_class("Datadog") is DatadogBackend
    assert get_backend_class("DATADOG") is DatadogBackend
