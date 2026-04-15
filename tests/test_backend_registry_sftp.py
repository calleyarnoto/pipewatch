"""Verify that SFTPBackend is registered in the backend registry."""

from __future__ import annotations

from pipewatch.backends import get_backend_class
from pipewatch.backends.sftp import SFTPBackend


def test_sftp_backend_is_registered():
    cls = get_backend_class("sftp")
    assert cls is SFTPBackend


def test_sftp_backend_name_case_insensitive():
    assert get_backend_class("SFTP") is SFTPBackend
    assert get_backend_class("Sftp") is SFTPBackend
