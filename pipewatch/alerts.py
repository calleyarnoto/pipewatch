"""Alert channel implementations for pipewatch."""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from pipewatch.backends.base import PipelineResult


@dataclass
class AlertMessage:
    """Represents an alert to be sent via a channel."""

    pipeline_name: str
    status: str
    message: str
    details: Optional[str] = None

    def format(self) -> str:
        parts = [f"[pipewatch] Pipeline '{self.pipeline_name}' is {self.status}"]
        parts.append(self.message)
        if self.details:
            parts.append(f"Details: {self.details}")
        return "\n".join(parts)


class BaseAlertChannel(ABC):
    """Abstract base class for alert channels."""

    @abstractmethod
    def send(self, alert: AlertMessage) -> bool:
        """Send an alert. Returns True on success."""
        ...

    @abstractmethod
    def name(self) -> str:
        """Return the channel name/type identifier."""
        ...


class LogAlertChannel(BaseAlertChannel):
    """Alert channel that logs to stdout (useful for testing and CLI output)."""

    def __init__(self, prefix: str = "ALERT"):
        self.prefix = prefix
        self._sent: list[AlertMessage] = []

    def send(self, alert: AlertMessage) -> bool:
        formatted = alert.format()
        print(f"[{self.prefix}] {formatted}")
        self._sent.append(alert)
        return True

    def name(self) -> str:
        return "log"

    @property
    def sent_alerts(self) -> list[AlertMessage]:
        return list(self._sent)


def build_alert_from_result(result: PipelineResult) -> AlertMessage:
    """Build an AlertMessage from a PipelineResult."""
    status_str = result.status.value
    message = (
        f"Pipeline check completed with status: {status_str.upper()}"
    )
    return AlertMessage(
        pipeline_name=result.pipeline_name,
        status=status_str,
        message=message,
        details=result.message,
    )


def get_alert_channel(channel_type: str, **kwargs) -> BaseAlertChannel:
    """Factory function to get an alert channel by type name."""
    channels = {
        "log": LogAlertChannel,
    }
    if channel_type not in channels:
        raise ValueError(
            f"Unknown alert channel: '{channel_type}'. "
            f"Available: {list(channels.keys())}"
        )
    return channels[channel_type](**kwargs)
