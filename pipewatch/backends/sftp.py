"""SFTP backend — checks pipeline health by counting files on a remote SFTP server."""

from __future__ import annotations

import logging
from typing import Any, Dict

from pipewatch.backends.base import BaseBackend, PipelineResult, PipelineStatus

logger = logging.getLogger(__name__)


class SFTPBackend(BaseBackend):
    """Check pipeline health by counting files in an SFTP directory.

    Config keys (passed via ``backend_config`` in AppConfig):
        host (str): SFTP hostname.
        port (int): SFTP port, default 22.
        username (str): SSH username.
        password (str, optional): SSH password.
        private_key_path (str, optional): Path to private key file.

    Pipeline-level config keys (``pipeline.config``):
        path (str): Remote directory path to list.
        pattern (str, optional): Glob-style filename filter (default ``"*"``).
        threshold (int): Minimum file count to be considered healthy (default 1).
    """

    def __init__(self, backend_config: Dict[str, Any]) -> None:
        self._host = backend_config["host"]
        self._port = int(backend_config.get("port", 22))
        self._username = backend_config["username"]
        self._password = backend_config.get("password")
        self._private_key_path = backend_config.get("private_key_path")

    def check_pipeline(self, pipeline: Any) -> PipelineResult:
        cfg = pipeline.config or {}
        remote_path = cfg.get("path", "/")
        pattern = cfg.get("pattern", "*")
        threshold = int(cfg.get("threshold", 1))

        try:
            import fnmatch

            import paramiko

            transport = paramiko.Transport((self._host, self._port))
            connect_kwargs: Dict[str, Any] = {"username": self._username}
            if self._private_key_path:
                connect_kwargs["pkey"] = paramiko.RSAKey.from_private_key_file(
                    self._private_key_path
                )
            else:
                connect_kwargs["password"] = self._password
            transport.connect(**connect_kwargs)

            sftp = paramiko.SFTPClient.from_transport(transport)
            try:
                all_files = sftp.listdir(remote_path)
            finally:
                sftp.close()
                transport.close()

            matched = [f for f in all_files if fnmatch.fnmatch(f, pattern)]
            count = len(matched)
            logger.debug(
                "SFTP %s:%s%s matched %d file(s) (pattern=%r, threshold=%d)",
                self._host,
                self._port,
                remote_path,
                count,
                pattern,
                threshold,
            )

            status = (
                PipelineStatus.HEALTHY if count >= threshold else PipelineStatus.FAILED
            )
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=status,
                detail=f"found {count} file(s) matching '{pattern}' in {remote_path}",
            )

        except Exception as exc:  # noqa: BLE001
            logger.warning("SFTPBackend error for %s: %s", pipeline.name, exc)
            return PipelineResult(
                pipeline_name=pipeline.name,
                status=PipelineStatus.UNKNOWN,
                detail=str(exc),
            )
