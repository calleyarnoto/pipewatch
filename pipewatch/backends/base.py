"""Base backend interface for pipeline health checks."""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Optional


class PipelineStatus(str, Enum):
    OK = "ok"
    WARNING = "warning"
    CRITICAL = "critical"
    UNKNOWN = "unknown"


@dataclass
class PipelineResult:
    """Result of a pipeline health check."""

    pipeline_name: str
    status: PipelineStatus
    last_run: Optional[datetime] = None
    message: str = ""
    metadata: dict = field(default_factory=dict)

    @property
    def is_healthy(self) -> bool:
        return self.status == PipelineStatus.OK

    def __str__(self) -> str:
        ts = self.last_run.isoformat() if self.last_run else "N/A"
        return (
            f"[{self.status.upper()}] {self.pipeline_name} "
            f"(last_run={ts}): {self.message}"
        )


class BaseBackend(ABC):
    """Abstract base class for all pipewatch backends."""

    def __init__(self, config: dict) -> None:
        self.config = config

    @abstractmethod
    def check_pipeline(self, pipeline_name: str) -> PipelineResult:
        """Check the health of a single pipeline."""
        ...

    def check_all(self, pipeline_names: list[str]) -> list[PipelineResult]:
        """Check health for multiple pipelines."""
        return [self.check_pipeline(name) for name in pipeline_names]

    @classmethod
    def from_config(cls, config: dict) -> "BaseBackend":
        """Instantiate backend from a config dict."""
        return cls(config)
