"""FTP backend — counts files in a remote directory and compares against a threshold."""

from __future__ import annotations

import ftplib
import logging
from typing import Any, Dict

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)

_DEFAULT_THRESHOLD = 1


class FTPBackend(BaseBackend):
    """Check pipeline health by counting files on an FTP server."""

    def __init__(self, config: Dict[str, Any]) -> None:
        self._host: str = config["host"]
        self._port: int = int(config.get("port", 21))
        self._username: str = config.get("username", "anonymous")
        self._password: str = config.get("password", "")
        self._passive: bool = bool(config.get("passive", True))

    def check_pipeline(self, pipeline: Dict[str, Any]) -> PipelineResult:
        name: str = pipeline["name"]
        directory: str = pipeline.get("directory", "/")
        pattern: str = pipeline.get("pattern", "")
        threshold: int = int(pipeline.get("threshold", _DEFAULT_THRESHOLD))

        try:
            ftp = ftplib.FTP()
            ftp.connect(self._host, self._port)
            ftp.login(self._username, self._password)
            ftp.set_pasv(self._passive)

            entries = ftp.nlst(directory)
            ftp.quit()

            if pattern:
                import fnmatch
                entries = [e for e in entries if fnmatch.fnmatch(e, pattern)]

            count = len(entries)
            logger.debug("FTP %s: found %d file(s) in %s", name, count, directory)

            status = PipelineStatus.HEALTHY if count >= threshold else PipelineStatus.FAILED
            return PipelineResult(
                pipeline_name=name,
                status=status,
                message=f"Found {count} file(s) (threshold={threshold})",
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("FTP check failed for %s: %s", name, exc)
            return PipelineResult(
                pipeline_name=name,
                status=PipelineStatus.UNKNOWN,
                message=str(exc),
            )
