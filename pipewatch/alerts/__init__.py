"""Alert channels for pipewatch."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Dict, Type

from pipewatch.backends.base import PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)

_ALERT_REGISTRY: Dict[str, Type["BaseAlertChannel"]] = {}


class AlertMessage:
    """Formats a PipelineResult into a human-readable alert message."""

    def __init__(self, result: PipelineResult):
        self.result = result

    def format(self) -> str:
        status_str = self.result.status.value.upper()
        lines = [
            f"Pipeline: {self.result.pipeline_name}",
            f"Status:   {status_str}",
        ]
        if self.result.message:
            lines.append(f"Details:  {self.result.message}")
        return "\n".join(lines)


class BaseAlertChannel(ABC):
    """Abstract base class for all alert channels."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique identifier for this alert channel."""

    @abstractmethod
    def send(self, result: PipelineResult) -> None:
        """Send an alert for the given pipeline result."""


def register_alert_channel(cls: Type[BaseAlertChannel]) -> Type[BaseAlertChannel]:
    """Decorator to register an alert channel class by name."""
    instance_name = cls.__name__
    _ALERT_REGISTRY[instance_name] = cls
    return cls


def get_alert_channel_class(name: str) -> Type[BaseAlertChannel]:
    """Return a registered alert channel class by its string name."""
    try:
        return _ALERT_REGISTRY[name]
    except KeyError:
        available = ", ".join(_ALERT_REGISTRY.keys())
        raise ValueError(
            f"Unknown alert channel '{name}'. Available: {available}"
        ) from None
