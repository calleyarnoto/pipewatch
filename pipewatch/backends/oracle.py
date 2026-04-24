"""Oracle database backend for pipewatch.

Checks pipeline health by executing a query against an Oracle database
and comparing the result against a configured threshold.

Requires: oracledb (pip install oracledb)

Config keys (under pipeline `options`):
    dsn         - Oracle DSN string, e.g. "host:port/service_name"
    user        - Database username
    password    - Database password
    query       - SQL query to execute; must return a single numeric value
    threshold   - Minimum value considered healthy (default: 1)
"""

from __future__ import annotations

import logging
from typing import Any

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)


class OracleBackend(BaseBackend):
    """Backend that queries an Oracle database to assess pipeline health."""

    def __init__(self, config: dict[str, Any]) -> None:
        """
        Initialise the Oracle backend.

        Args:
            config: Top-level backend configuration dict.  Connection
                    defaults (dsn, user, password) may be provided here
                    and overridden per-pipeline via ``options``.
        """
        self._default_dsn = config.get("dsn", "")
        self._default_user = config.get("user", "")
        self._default_password = config.get("password", "")

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def check_pipeline(self, pipeline) -> PipelineResult:
        """Run the configured query and evaluate the result.

        Args:
            pipeline: A :class:`~pipewatch.config.PipelineConfig` instance.

        Returns:
            A :class:`~pipewatch.backends.base.PipelineResult` reflecting
            the health of the pipeline.
        """
        try:
            import oracledb  # type: ignore[import]
        except ImportError:
            logger.error(
                "oracledb package is not installed. "
                "Run: pip install oracledb"
            )
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="oracledb package not installed",
            )

        opts = pipeline.options or {}
        dsn = opts.get("dsn", self._default_dsn)
        user = opts.get("user", self._default_user)
        password = opts.get("password", self._default_password)
        query = opts.get("query", "")
        threshold = int(opts.get("threshold", 1))

        if not query:
            logger.error("No query configured for pipeline '%s'", pipeline.name)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="No query configured",
            )

        try:
            with oracledb.connect(user=user, password=password, dsn=dsn) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(query)
                    row = cursor.fetchone()

            if row is None:
                logger.warning(
                    "Query returned no rows for pipeline '%s'", pipeline.name
                )
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.UNKNOWN,
                    message="Query returned no rows",
                )

            value = row[0]
            logger.debug(
                "Pipeline '%s' query returned value=%s (threshold=%s)",
                pipeline.name,
                value,
                threshold,
            )

            if value is None:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.UNKNOWN,
                    message="Query returned NULL",
                )

            numeric_value = float(value)
            if numeric_value >= threshold:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.HEALTHY,
                    message=f"value={numeric_value} >= threshold={threshold}",
                )

            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.FAILED,
                message=f"value={numeric_value} < threshold={threshold}",
            )

        except Exception as exc:  # pylint: disable=broad-except
            logger.exception(
                "Error checking pipeline '%s' via Oracle: %s",
                pipeline.name,
                exc,
            )
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=f"Exception: {exc}",
            )
