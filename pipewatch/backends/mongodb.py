"""MongoDB backend for pipewatch.

Checks pipeline health by querying a MongoDB collection and verifying
that the document count meets a configured threshold.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus


class MongoDBBackend(BaseBackend):
    """Backend that queries a MongoDB collection for pipeline health."""

    def __init__(self, config: dict[str, Any]) -> None:
        try:
            import pymongo  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "pymongo is required for the MongoDB backend: pip install pymongo"
            ) from exc

        self._uri: str = config.get("uri", "mongodb://localhost:27017")
        self._database: str = config["database"]
        self._collection: str = config["collection"]
        self._default_threshold: int = int(config.get("threshold", 1))
        self._filter: dict[str, Any] = config.get("filter", {})

    def check_pipeline(self, pipeline_config: Any) -> PipelineResult:
        """Return a PipelineResult based on document count in the collection."""
        import pymongo

        name: str = pipeline_config.name
        threshold: int = int(
            getattr(pipeline_config, "threshold", None) or self._default_threshold
        )

        try:
            client: pymongo.MongoClient = pymongo.MongoClient(
                self._uri, serverSelectionTimeoutMS=5000
            )
            db = client[self._database]
            count: int = db[self._collection].count_documents(self._filter)
            client.close()
        except Exception as exc:  # pylint: disable=broad-except
            return PipelineResult(
                pipeline_name=name,
                status=PipelineStatus.UNKNOWN,
                checked_at=datetime.now(tz=timezone.utc),
                message=f"MongoDB error: {exc}",
            )

        if count >= threshold:
            status = PipelineStatus.HEALTHY
            message = f"Document count {count} meets threshold {threshold}"
        else:
            status = PipelineStatus.FAILED
            message = f"Document count {count} is below threshold {threshold}"

        return PipelineResult(
            pipeline_name=name,
            status=status,
            checked_at=datetime.now(tz=timezone.utc),
            message=message,
        )
