"""Tests for the alert channel registry."""

import pytest

from pipewatch.alerts import BaseAlertChannel
from pipewatch.alerts.registry import (
    _registry,
    get_channel_class,
    register_channel,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _DummyChannel(BaseAlertChannel):
    @property
    def name(self) -> str:  # type: ignore[override]
        return "dummy"

    def send(self, message) -> None:  # type: ignore[override]
        pass


@pytest.fixture(autouse=True)
def _clear_registry():
    """Ensure the registry is cleared between tests."""
    _registry.clear()
    yield
    _registry.clear()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_builtin_channels_registered():
    """All built-in channel names should be present after first access."""
    builtins = [
        "slack", "email", "pagerduty", "webhook",
        "opsgenie", "victorops", "teams", "discord",
    ]
    for channel_name in builtins:
        cls = get_channel_class(channel_name)
        assert cls is not None


def test_get_unknown_channel_raises_key_error():
    """Requesting an unregistered channel name should raise KeyError."""
    with pytest.raises(KeyError, match="Unknown alert channel 'nonexistent'"):
        get_channel_class("nonexistent")


def test_error_message_lists_available_channels():
    """The KeyError message should list known channels to aid the user."""
    with pytest.raises(KeyError, match="slack"):
        get_channel_class("not_a_channel")


def test_register_custom_channel():
    """A custom channel class can be registered and retrieved."""
    register_channel("dummy", _DummyChannel)
    cls = get_channel_class("dummy")
    assert cls is _DummyChannel


def test_registered_class_is_subclass_of_base():
    """Every registered built-in class should subclass BaseAlertChannel."""
    cls = get_channel_class("slack")
    assert issubclass(cls, BaseAlertChannel)


def test_register_channel_overwrites_existing():
    """Re-registering under the same name replaces the previous entry."""
    register_channel("dummy", BaseAlertChannel)  # type: ignore[type-abstract]
    register_channel("dummy", _DummyChannel)
    assert get_channel_class("dummy") is _DummyChannel
