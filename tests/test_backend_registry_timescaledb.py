"""Verify the TimescaleDB backend is registered correctly."""

import importlib

from pipewatch.backends import get_backend_class
from pipewatch.backends.timescaledb import TimescaleDBBackend


def test_timescaledb_backend_is_registered():
    importlib.import_module("pipewatch.backends.timescaledb_register")
    cls = get_backend_class("timescaledb")
    assert cls is TimescaleDBBackend


def test_timescaledb_backend_name_case_insensitive():
    importlib.import_module("pipewatch.backends.timescaledb_register")
    cls = get_backend_class("TimescaleDB")
    assert cls is TimescaleDBBackend
