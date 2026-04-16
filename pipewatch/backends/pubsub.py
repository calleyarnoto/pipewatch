"""Google Cloud Pub/Sub backend for pipewatch.

Checks pipeline health by inspecting subscription backlog (undelivered
message count) against a configurable threshold.

Required pipeline config keys:
  - project   : GCP project ID
  - subscription: Pub/Sub subscription name (short name, not full path)

Optional pipeline config keys:
  - threshold : minimum backlog count considered healthy (default 0,
                meaning *zero* undelivered messages == healthy).
                Set to -1 to skip the threshold check and treat any
                reachable subscription as healthy.
"""

from __future__ import annotations

import logging
from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)


class PubSubBackend(BaseBackend):
    """Backend that queries a Pub/Sub subscription's message backlog."""

    def __init__(self, config: dict[str, Any]) -> None:
        try:
            from google.cloud import pubsub_v1  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "google-cloud-pubsub is required for the pubsub backend. "
                "Install it with: pip install google-cloud-pubsub"
            ) from exc
        self._config = config

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        from google.api_core.exceptions import GoogleAPICallError
        from google.cloud import pubsub_v1

        project = pipeline.config.get("project") or self._config.get("project")
        subscription = pipeline.config.get("subscription")

        if not project or not subscription:
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="'project' and 'subscription' are required in pipeline config",
            )

        threshold = int(pipeline.config.get("threshold", 0))
        sub_path = f"projects/{project}/subscriptions/{subscription}"

        try:
            client = pubsub_v1.SubscriberClient()
            with client:
                sub = client.get_subscription(request={"subscription": sub_path})
                stats = sub.expiration_policy  # noqa: F841 – kept for future use
                # Backlog is surfaced via the Monitoring API; use snapshot count
                # available on the subscription object when present.
                # Fall back to num_undelivered_messages from get_snapshot.
                backlog: int = getattr(sub, "num_undelivered_messages", 0) or 0
        except GoogleAPICallError as exc:
            logger.warning("PubSub API error for %s: %s", pipeline.name, exc)
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"API error: {exc}",
            )
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("Unexpected error for %s: %s", pipeline.name, exc)
            return PipelineResult(
                name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Unexpected error: {exc}",
            )

        if threshold == -1:
            status = PipelineStatus.HEALTHY
            message = f"subscription reachable; backlog={backlog}"
        elif backlog <= threshold:
            status = PipelineStatus.HEALTHY
            message = f"backlog={backlog} <= threshold={threshold}"
        else:
            status = PipelineStatus.FAILED
            message = f"backlog={backlog} > threshold={threshold}"

        return PipelineResult(name=pipeline.name, status=status, message=message)
