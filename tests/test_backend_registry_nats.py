"""Verify that the NATS backend registers correctly."""

import importlib

import pytest

from pipewatch.backends import get_backend_class


def test_nats_backend_is_registered():
    # Trigger registration via the register module.
    importlib.import_module("pipewatch.backends.nats_register")
    cls = get_backend_class("nats")
    from pipewatch.backends.nats import NATSBackend

    assert cls is NATSBackend


def test_nats_backend_name_case_insensitive():
    importlib.import_module("pipewatch.backends.nats_register")
    cls = get_backend_class("NATS")
    from pipewatch.backends.nats import NATSBackend

    assert cls is NATSBackend
