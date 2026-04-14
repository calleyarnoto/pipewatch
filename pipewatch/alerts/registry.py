"""Registry for alert channel backends."""

from __future__ import annotations

from typing import Dict, Type

from pipewatch.alerts import BaseAlertChannel

_registry: Dict[str, Type[BaseAlertChannel]] = {}


def register_channel(name: str, cls: Type[BaseAlertChannel]) -> None:
    """Register an alert channel class under *name*."""
    _registry[name] = cls


def get_channel_class(name: str) -> Type[BaseAlertChannel]:
    """Return the alert channel class registered under *name*.

    Raises
    ------
    KeyError
        If *name* has not been registered.
    """
    _register_builtins()
    if name not in _registry:
        available = ", ".join(sorted(_registry))
        raise KeyError(
            f"Unknown alert channel '{name}'. Available channels: {available}"
        )
    return _registry[name]


def _register_builtins() -> None:
    """Lazily register all built-in alert channels."""
    if _registry:
        return

    from pipewatch.alerts.slack import SlackAlertChannel
    from pipewatch.alerts.email import EmailAlertChannel
    from pipewatch.alerts.pagerduty import PagerDutyAlertChannel
    from pipewatch.alerts.webhook import WebhookAlertChannel
    from pipewatch.alerts.opsgenie import OpsGenieAlertChannel
    from pipewatch.alerts.victorops import VictorOpsAlertChannel
    from pipewatch.alerts.teams import TeamsAlertChannel
    from pipewatch.alerts.discord import DiscordAlertChannel

    register_channel("slack", SlackAlertChannel)
    register_channel("email", EmailAlertChannel)
    register_channel("pagerduty", PagerDutyAlertChannel)
    register_channel("webhook", WebhookAlertChannel)
    register_channel("opsgenie", OpsGenieAlertChannel)
    register_channel("victorops", VictorOpsAlertChannel)
    register_channel("teams", TeamsAlertChannel)
    register_channel("discord", DiscordAlertChannel)
