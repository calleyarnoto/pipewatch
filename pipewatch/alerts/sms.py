"""SMS alert channel via Twilio."""

from __future__ import annotations

import logging
from typing import Any

from pipewatch.alerts import BaseAlertChannel, AlertMessage
from pipewatch.backends.base import PipelineResult

logger = logging.getLogger(__name__)


class SMSAlertChannel(BaseAlertChannel):
    """Send pipeline alerts as SMS messages using the Twilio REST API."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._account_sid: str = config["account_sid"]
        self._auth_token: str = config["auth_token"]
        self._from_number: str = config["from_number"]
        self._to_numbers: list[str] = config["to_numbers"]
        self._only_failures: bool = config.get("only_failures", True)

    @property
    def name(self) -> str:
        return "sms"

    def _build_body(self, result: PipelineResult) -> str:
        msg = AlertMessage(result)
        return msg.format()

    def send(self, result: PipelineResult) -> None:
        if self._only_failures and result.is_healthy:
            logger.debug(
                "SMS channel skipping healthy result for %s", result.pipeline_name
            )
            return

        try:
            from twilio.rest import Client  # type: ignore[import]
        except ImportError as exc:  # pragma: no cover
            raise ImportError(
                "twilio package is required for the SMS alert channel. "
                "Install it with: pip install twilio"
            ) from exc

        client = Client(self._account_sid, self._auth_token)
        body = self._build_body(result)

        for number in self._to_numbers:
            client.messages.create(
                body=body,
                from_=self._from_number,
                to=number,
            )
            logger.info(
                "SMS alert sent to %s for pipeline %s",
                number,
                result.pipeline_name,
            )
