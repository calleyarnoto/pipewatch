"""Registry for alert channel implementations."""

from __future__ import annotations

import logging
from typing import Type

from pipewatch.alerts import BaseAlertChannel

logger = logging.getLogger(__name__)

_REGISTRY: dict[str, Type[BaseAlertChannel]] = {}


def register_channel(name: str, cls: Type[BaseAlertChannel]) -> None:
    """Register an alert channel class under *name*."""
    if name in _REGISTRY:
        logger.debug("Overwriting alert channel registration for '%s'", name)
    _REGISTRY[name] = cls
    logger.debug("Registered alert channel '%s' -> %s", name, cls.__qualname__)


def get_channel_class(name: str) -> Type[BaseAlertChannel]:
    """Return the channel class registered under *name*.

    Raises
    ------
    KeyError
        If no channel is registered under *name*.
    """
    if name not in _REGISTRY:
        available = ", ".join(sorted(_REGISTRY))
        raise KeyError(
            f"Unknown alert channel '{name}'. Available channels: {available}"
        )
    return _REGISTRY[name]


def _register_builtins() -> None:
    """Register all built-in alert channel implementations."""
    from pipewatch.alerts.slack import SlackAlertChannel
    from pipewatch.alerts.email import EmailAlertChannel
    from pipewatch.alerts.pagerduty import PagerDutyAlertChannel
    from pipewatch.alerts.webhook import WebhookAlertChannel
    from pipewatch.alerts.opsgenie import OpsGenieAlertChannel
    from pipewatch.alerts.victorops import VictorOpsAlertChannel
    from pipewatch.alerts.teams import TeamsAlertChannel
    from pipewatch.alerts.discord import DiscordAlertChannel
    from pipewatch.alerts.sms import SMSAlertChannel

    _builtins: list[tuple[str, Type[BaseAlertChannel]]] = [
        ("slack", SlackAlertChannel),
        ("email", EmailAlertChannel),
        ("pagerduty", PagerDutyAlertChannel),
        ("webhook", WebhookAlertChannel),
        ("opsgenie", OpsGenieAlertChannel),
        ("victorops", VictorOpsAlertChannel),
        ("teams", TeamsAlertChannel),
        ("discord", DiscordAlertChannel),
        ("sms", SMSAlertChannel),
    ]
    for channel_name, cls in _builtins:
        register_channel(channel_name, cls)


_register_builtins()
