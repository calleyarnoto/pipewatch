"""Tableau backend — checks whether a datasource extract has refreshed recently."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import requests

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)

_DEFAULT_MAX_AGE_HOURS = 24


class TableauBackend(BaseBackend):
    """Query the Tableau REST API to verify a datasource was recently refreshed.

    Required pipeline config keys:
        datasource_id (str): LUID of the Tableau datasource.

    Optional pipeline config keys:
        max_age_hours (int): Maximum acceptable age of the last refresh in hours.
                             Defaults to 24.

    Required backend config keys:
        server_url (str): Base URL of the Tableau server, e.g. https://tableau.example.com
        token_name (str): Personal access token name.
        token_value (str): Personal access token secret.
        site_id (str): Tableau site content URL (empty string for Default site).
    """

    def __init__(self, config: dict[str, Any]) -> None:
        self._server_url = config["server_url"].rstrip("/")
        self._token_name = config["token_name"]
        self._token_value = config["token_value"]
        self._site_id = config.get("site_id", "")
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})
        self._api_version = config.get("api_version", "3.21")

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _base(self) -> str:
        return f"{self._server_url}/api/{self._api_version}"

    def _sign_in(self) -> str:
        """Authenticate and return a session token."""
        url = f"{self._base()}/auth/signin"
        payload = {
            "credentials": {
                "personalAccessTokenName": self._token_name,
                "personalAccessTokenSecret": self._token_value,
                "site": {"contentUrl": self._site_id},
            }
        }
        resp = self._session.post(url, json=payload, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        token = data["credentials"]["token"]
        site_luid = data["credentials"]["site"]["id"]
        return token, site_luid

    # ------------------------------------------------------------------
    # BaseBackend interface
    # ------------------------------------------------------------------

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        datasource_id = pipeline.config.get("datasource_id")
        if not datasource_id:
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message="datasource_id not configured",
            )

        max_age_hours = int(pipeline.config.get("max_age_hours", _DEFAULT_MAX_AGE_HOURS))

        try:
            token, site_luid = self._sign_in()
            self._session.headers["x-tableau-auth"] = token

            url = f"{self._base()}/sites/{site_luid}/datasources/{datasource_id}"
            resp = self._session.get(url, timeout=15)
            resp.raise_for_status()

            updated_at_str = resp.json()["datasource"].get("updatedAt", "")
            if not updated_at_str:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.UNKNOWN,
                    message="updatedAt field missing from API response",
                )

            updated_at = datetime.fromisoformat(updated_at_str.replace("Z", "+00:00"))
            age_hours = (datetime.now(timezone.utc) - updated_at).total_seconds() / 3600

            if age_hours <= max_age_hours:
                return PipelineResult(
                    pipeline_name=pipeline.name,
                    status=PipelineStatus.HEALTHY,
                    message=f"Datasource refreshed {age_hours:.1f}h ago (threshold {max_age_hours}h)",
                )
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.FAILED,
                message=f"Datasource last refreshed {age_hours:.1f}h ago, exceeds {max_age_hours}h threshold",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("TableauBackend error for %s: %s", pipeline.name, exc)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                message=str(exc),
            )
