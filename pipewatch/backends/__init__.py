"""Backend registry for pipewatch."""

from __future__ import annotations

from typing import TYPE_CHECKING, Type

if TYPE_CHECKING:
    from pipewatch.backends.base import BaseBackend

_REGISTRY: dict[str, Type["BaseBackend"]] = {}


def register_backend(name: str, cls: Type["BaseBackend"]) -> None:
    """Register a backend class under the given name."""
    _REGISTRY[name] = cls


def get_backend_class(name: str) -> Type["BaseBackend"]:
    """Return the backend class for *name*, raising KeyError if unknown."""
    _register_builtins()
    if name not in _REGISTRY:
        raise KeyError(
            f"Unknown backend '{name}'. Available: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]


def _register_builtins() -> None:
    """Lazily import and register all built-in backends."""
    if _REGISTRY:
        return

    from pipewatch.backends.dummy import DummyBackend
    from pipewatch.backends.airflow import AirflowBackend
    from pipewatch.backends.prometheus import PrometheusBackend
    from pipewatch.backends.postgres import PostgresBackend
    from pipewatch.backends.mysql import MySQLBackend
    from pipewatch.backends.bigquery import BigQueryBackend

    register_backend("dummy", DummyBackend)
    register_backend("airflow", AirflowBackend)
    register_backend("prometheus", PrometheusBackend)
    register_backend("postgres", PostgresBackend)
    register_backend("mysql", MySQLBackend)
    register_backend("bigquery", BigQueryBackend)
