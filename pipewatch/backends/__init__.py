"""Backend registry for pipewatch.

Backends are registered by their ``name`` attribute so that config files can
refer to them by string (e.g. ``backend: airflow``).
"""

from __future__ import annotations

from typing import Dict, Type

from pipewatch.backends.base import BaseBackend
from pipewatch.backends.dummy import DummyBackend
from pipewatch.backends.airflow import AirflowBackend

_REGISTRY: Dict[str, Type[BaseBackend]] = {
    DummyBackend.name: DummyBackend,
    AirflowBackend.name: AirflowBackend,
}


def get_backend_class(name: str) -> Type[BaseBackend]:
    """Return the backend class registered under *name*.

    Raises
    ------
    KeyError
        If no backend with that name has been registered.
    """
    try:
        return _REGISTRY[name]
    except KeyError:
        available = ", ".join(sorted(_REGISTRY))
        raise KeyError(
            f"Unknown backend {name!r}. Available backends: {available}"
        ) from None


def register_backend(cls: Type[BaseBackend]) -> Type[BaseBackend]:
    """Register a custom backend class (decorator or direct call).

    Example
    -------
    >>> from pipewatch.backends import register_backend
    >>> from pipewatch.backends.base import BaseBackend
    >>> @register_backend
    ... class MyBackend(BaseBackend):
    ...     name = "my_backend"
    ...     def check_pipeline(self, pipeline_id, **kwargs): ...
    """
    _REGISTRY[cls.name] = cls
    return cls


__all__ = [
    "BaseBackend",
    "DummyBackend",
    "AirflowBackend",
    "get_backend_class",
    "register_backend",
]
