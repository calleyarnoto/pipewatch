"""Backend registry for pipewatch.

Built-in backends are registered here; third-party backends can call
``register_backend`` to add their own.
"""

from __future__ import annotations

from typing import Dict, Type

from pipewatch.backends.base import BaseBackend

_REGISTRY: Dict[str, Type[BaseBackend]] = {}


def register_backend(name: str, cls: Type[BaseBackend]) -> None:
    """Register a backend class under *name*."""
    _REGISTRY[name.lower()] = cls


def get_backend_class(name: str) -> Type[BaseBackend]:
    """Return the backend class registered under *name*.

    Raises
    ------
    KeyError
        If no backend with that name is registered.
    """
    key = name.lower()
    if key not in _REGISTRY:
        raise KeyError(
            f"Unknown backend '{name}'. Available backends: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[key]


def _register_builtins() -> None:
    """Lazily import and register all built-in backends."""
    from pipewatch.backends.airflow import AirflowBackend
    from pipewatch.backends.dummy import DummyBackend
    from pipewatch.backends.postgres import PostgresBackend
    from pipewatch.backends.prometheus import PrometheusBackend

    register_backend("airflow", AirflowBackend)
    register_backend("dummy", DummyBackend)
    register_backend("postgres", PostgresBackend)
    register_backend("prometheus", PrometheusBackend)


_register_builtins()

__all__ = ["get_backend_class", "register_backend"]
