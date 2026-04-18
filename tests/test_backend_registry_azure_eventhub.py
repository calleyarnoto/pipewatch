"""Registry tests for AzureEventHubBackend."""
from pipewatch.backends import get_backend_class
from pipewatch.backends.azure_eventhub import AzureEventHubBackend


def test_azure_eventhub_backend_is_registered():
    cls = get_backend_class("azure_eventhub")
    assert cls is AzureEventHubBackend


def test_azure_eventhub_backend_name_case_insensitive():
    cls = get_backend_class("Azure_EventHub")
    assert cls is AzureEventHubBackend
