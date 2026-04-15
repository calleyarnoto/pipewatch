"""Celery backend — checks pipeline health via Celery Inspect API."""
from __future__ import annotations

from typing import Any, Dict

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class CeleryBackend(BaseBackend):
    """Backend that queries a Celery app's active/reserved task queues.

    Pipeline config extras:
        queue (str): Name of the queue to inspect.  Required.
        max_active (int): Alert if active task count exceeds this value.
            Defaults to None (no upper-bound check).
        min_active (int): Alert if active task count is below this value.
            Defaults to 0 (zero tasks is still healthy).
    """

    def __init__(self, config: Dict[str, Any]) -> None:
        try:
            from celery import Celery  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "celery package is required for CeleryBackend. "
                "Install it with: pip install celery"
            ) from exc

        broker_url = config.get("broker_url", "redis://localhost:6379/0")
        app_name = config.get("app_name", "pipewatch")
        from celery import Celery

        self._app = Celery(app_name, broker=broker_url)
        self._app.config_from_object({"broker_url": broker_url})

    def check_pipeline(self, pipeline) -> PipelineResult:
        queue: str = pipeline.extras.get("queue", "")
        if not queue:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'queue' is required in pipeline extras for CeleryBackend",
            )

        max_active: int | None = pipeline.extras.get("max_active")
        min_active: int = pipeline.extras.get("min_active", 0)

        try:
            inspect = self._app.control.inspect(timeout=5)
            active: Dict[str, list] = inspect.active() or {}
        except Exception as exc:  # noqa: BLE001
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Celery inspect failed: {exc}",
            )

        count = sum(
            1
            for tasks in active.values()
            for task in tasks
            if task.get("delivery_info", {}).get("routing_key") == queue
            or task.get("delivery_info", {}).get("exchange") == queue
        )

        if count < min_active:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.FAILED,
                message=(
                    f"Active task count {count} is below min_active={min_active} "
                    f"for queue '{queue}'"
                ),
            )

        if max_active is not None and count > max_active:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.FAILED,
                message=(
                    f"Active task count {count} exceeds max_active={max_active} "
                    f"for queue '{queue}'"
                ),
            )

        return PipelineResult(
            name=pipeline.name,
            status=PipelineStatus.HEALTHY,
            message=f"Queue '{queue}' has {count} active task(s)",
        )
