"""Dummy backend for testing and development."""

from datetime import datetime, timezone
from typing import Optional

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class DummyBackend(BaseBackend):
    """A no-op backend that returns configurable static results.

    Useful for local development, CI, and unit tests.

    Config keys:
        default_status (str): Status to return for all pipelines. Default: 'ok'.
        pipelines (dict): Per-pipeline overrides, e.g.:
            {"my_pipeline": {"status": "warning", "message": "slow run"}}
    """

    def check_pipeline(self, pipeline_name: str) -> PipelineResult:
        pipeline_overrides: dict = self.config.get("pipelines", {}).get(
            pipeline_name, {}
        )
        raw_status = pipeline_overrides.get(
            "status", self.config.get("default_status", "ok")
        )
        try:
            status = PipelineStatus(raw_status)
        except ValueError:
            status = PipelineStatus.UNKNOWN

        message = pipeline_overrides.get("message", f"Dummy check for {pipeline_name}")
        last_run_str: Optional[str] = pipeline_overrides.get("last_run")
        last_run = (
            datetime.fromisoformat(last_run_str)
            if last_run_str
            else datetime.now(tz=timezone.utc)
        )

        return PipelineResult(
            pipeline_name=pipeline_name,
            status=status,
            last_run=last_run,
            message=message,
            metadata={"backend": "dummy"},
        )
