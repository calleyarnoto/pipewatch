"""Auto-registration shim for TrinoBackend.

Imported by pipewatch/backends/__init__.py inside _register_builtins.
"""
from pipewatch.backends import register_backend
from pipewatch.backends.trino import TrinoBackend

register_backend("trino", TrinoBackend)
