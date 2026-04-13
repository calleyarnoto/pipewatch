"""Pipeline check runner — ties together backends, config, and alerts."""

from dataclasses import dataclass, field
from typing import Optional

from pipewatch.alerts import BaseAlertChannel, build_alert_from_result, get_alert_channel
from pipewatch.backends.base import BaseBackend, PipelineResult, is_healthy
from pipewatch.config import AppConfig


@dataclass
class RunReport:
    """Summary of a full pipeline check run."""

    results: list[PipelineResult] = field(default_factory=list)
    alerts_sent: int = 0

    @property
    def total(self) -> int:
        return len(self.results)

    @property
    def healthy(self) -> int:
        return sum(1 for r in self.results if is_healthy(r))

    @property
    def unhealthy(self) -> int:
        return self.total - self.healthy

    def summary(self) -> str:
        return (
            f"Checked {self.total} pipeline(s): "
            f"{self.healthy} healthy, {self.unhealthy} unhealthy. "
            f"Alerts sent: {self.alerts_sent}."
        )


class PipelineRunner:
    """Runs pipeline health checks and dispatches alerts."""

    def __init__(
        self,
        backend: BaseBackend,
        alert_channels: Optional[list[BaseAlertChannel]] = None,
        alert_on_healthy: bool = False,
    ):
        self.backend = backend
        self.alert_channels: list[BaseAlertChannel] = alert_channels or []
        self.alert_on_healthy = alert_on_healthy

    def run(self, pipeline_names: list[str]) -> RunReport:
        """Check each pipeline and send alerts for unhealthy results."""
        report = RunReport()
        for name in pipeline_names:
            result = self.backend.check_pipeline(name)
            report.results.append(result)
            should_alert = not is_healthy(result) or self.alert_on_healthy
            if should_alert and self.alert_channels:
                alert = build_alert_from_result(result)
                for channel in self.alert_channels:
                    if channel.send(alert):
                        report.alerts_sent += 1
        return report

    @classmethod
    def from_config(cls, config: AppConfig, backend: BaseBackend) -> "PipelineRunner":
        """Build a PipelineRunner from an AppConfig and a backend instance."""
        channels = [
            get_alert_channel(ch) for ch in (config.alert_channels or [])
        ]
        return cls(backend=backend, alert_channels=channels)
