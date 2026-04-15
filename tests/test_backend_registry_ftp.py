"""Verify FTPBackend is registered in the backend registry."""

from __future__ import annotations

from pipewatch.backends import get_backend_class
from pipewatch.backends.ftp import FTPBackend


def test_ftp_backend_is_registered():
    cls = get_backend_class("ftp")
    assert cls is FTPBackend


def test_ftp_backend_name_case_insensitive():
    assert get_backend_class("FTP") is FTPBackend
    assert get_backend_class("Ftp") is FTPBackend
