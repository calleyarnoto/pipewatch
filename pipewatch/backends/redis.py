"""Redis backend for pipewatch — checks pipeline health via Redis key existence or value thresholds."""

from __future__ import annotations

from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class RedisBackend(BaseBackend):
    """Check pipeline health by inspecting a Redis key's value.

    Config keys (under ``backend_config`` in pipeline or global config):
      - ``host`` (str): Redis host. Default ``"localhost"``.
      - ``port`` (int): Redis port. Default ``6379``.
      - ``db`` (int): Redis database index. Default ``0``.
      - ``password`` (str | None): Optional password.
      - ``key`` (str): The Redis key to inspect (required per-pipeline).
      - ``threshold`` (int): Minimum value to be considered healthy. Default ``1``.
    """

    def __init__(self, config: dict[str, Any]) -> None:
        try:
            import redis  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "redis-py is required for RedisBackend: pip install redis"
            ) from exc

        self._redis = redis.Redis(
            host=config.get("host", "localhost"),
            port=int(config.get("port", 6379)),
            db=int(config.get("db", 0)),
            password=config.get("password"),
            decode_responses=True,
        )
        self._default_threshold: int = int(config.get("threshold", 1))

    def check_pipeline(self, pipeline_cfg: Any) -> PipelineResult:
        """Return a PipelineResult based on the numeric value stored at *key*.

        The pipeline is **healthy** when ``int(value) >= threshold``.
        If the key does not exist or the value cannot be cast to int the
        status is **unknown**.
        """
        extra: dict[str, Any] = getattr(pipeline_cfg, "backend_config", {}) or {}
        key: str | None = extra.get("key")
        threshold: int = int(extra.get("threshold", self._default_threshold))

        if not key:
            return PipelineResult(
                pipeline_name=pipeline_cfg.name,
                status=PipelineStatus.UNKNOWN,
                message="No Redis key configured for this pipeline.",
            )

        try:
            raw = self._redis.get(key)
        except Exception as exc:  # pragma: no cover
            return PipelineResult(
                pipeline_name=pipeline_cfg.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Redis error: {exc}",
            )

        if raw is None:
            return PipelineResult(
                pipeline_name=pipeline_cfg.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Key '{key}' does not exist in Redis.",
            )

        try:
            value = int(raw)
        except (TypeError, ValueError):
            return PipelineResult(
                pipeline_name=pipeline_cfg.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Key '{key}' value '{raw}' is not numeric.",
            )

        status = PipelineStatus.HEALTHY if value >= threshold else PipelineStatus.FAILED
        return PipelineResult(
            pipeline_name=pipeline_cfg.name,
            status=status,
            message=f"Key '{key}' = {value} (threshold={threshold}).",
        )
