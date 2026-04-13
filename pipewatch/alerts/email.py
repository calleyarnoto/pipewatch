"""Email alert channel for pipewatch."""

import smtplib
import logging
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from typing import List

from pipewatch.alerts import BaseAlertChannel, AlertMessage
from pipewatch.backends.base import PipelineResult

logger = logging.getLogger(__name__)


class EmailAlertChannel(BaseAlertChannel):
    """Alert channel that sends notifications via SMTP email."""

    def __init__(
        self,
        recipients: List[str],
        sender: str,
        smtp_host: str = "localhost",
        smtp_port: int = 25,
        username: str = None,
        password: str = None,
        use_tls: bool = False,
    ):
        self.recipients = recipients
        self.sender = sender
        self.smtp_host = smtp_host
        self.smtp_port = smtp_port
        self.username = username
        self.password = password
        self.use_tls = use_tls

    @property
    def name(self) -> str:
        return "email"

    def _build_message(self, result: PipelineResult) -> MIMEMultipart:
        alert = AlertMessage(result)
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[pipewatch] Pipeline alert: {result.pipeline_name}"
        msg["From"] = self.sender
        msg["To"] = ", ".join(self.recipients)
        body = MIMEText(alert.format(), "plain")
        msg.attach(body)
        return msg

    def send(self, result: PipelineResult) -> None:
        msg = self._build_message(result)
        try:
            if self.use_tls:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)
                server.starttls()
            else:
                server = smtplib.SMTP(self.smtp_host, self.smtp_port)
            if self.username and self.password:
                server.login(self.username, self.password)
            server.sendmail(self.sender, self.recipients, msg.as_string())
            server.quit()
            logger.info(
                "Email alert sent for pipeline '%s' to %s",
                result.pipeline_name,
                self.recipients,
            )
        except smtplib.SMTPException as exc:
            logger.error("Failed to send email alert: %s", exc)
            raise
