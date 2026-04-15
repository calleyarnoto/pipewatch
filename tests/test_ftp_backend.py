"""Tests for the FTP backend."""

from __future__ import annotations

import ftplib
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.ftp import FTPBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend() -> FTPBackend:
    return FTPBackend(
        {
            "host": "ftp.example.com",
            "port": 21,
            "username": "user",
            "password": "secret",
        }
    )


@pytest.fixture()
def _pipeline() -> dict:
    return {"name": "daily_export", "directory": "/exports", "threshold": 1}


def _make_ftp_mock(entries):
    ftp = MagicMock()
    ftp.nlst.return_value = entries
    return ftp


def test_healthy_when_count_meets_threshold(backend, _pipeline):
    with patch("pipewatch.backends.ftp.ftplib.FTP") as MockFTP:
        MockFTP.return_value = _make_ftp_mock(["/exports/file1.csv", "/exports/file2.csv"])
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_failed_when_count_below_threshold(backend, _pipeline):
    with patch("pipewatch.backends.ftp.ftplib.FTP") as MockFTP:
        MockFTP.return_value = _make_ftp_mock([])
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.FAILED


def test_custom_threshold(backend):
    pipeline = {"name": "big_export", "directory": "/exports", "threshold": 5}
    with patch("pipewatch.backends.ftp.ftplib.FTP") as MockFTP:
        MockFTP.return_value = _make_ftp_mock(["/exports/f.csv"] * 5)
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.HEALTHY


def test_pattern_filters_entries(backend):
    pipeline = {"name": "csv_only", "directory": "/exports", "pattern": "*.csv", "threshold": 1}
    entries = ["/exports/report.csv", "/exports/report.json"]
    with patch("pipewatch.backends.ftp.ftplib.FTP") as MockFTP:
        MockFTP.return_value = _make_ftp_mock(entries)
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.HEALTHY
    assert "1 file" in result.message


def test_unknown_on_connection_error(backend, _pipeline):
    with patch("pipewatch.backends.ftp.ftplib.FTP") as MockFTP:
        MockFTP.return_value.connect.side_effect = ftplib.error_temp("timeout")
        result = backend.check_pipeline(_pipeline)
    assert result.status == PipelineStatus.UNKNOWN


def test_message_contains_count_and_threshold(backend, _pipeline):
    with patch("pipewatch.backends.ftp.ftplib.FTP") as MockFTP:
        MockFTP.return_value = _make_ftp_mock(["/exports/a.csv"])
        result = backend.check_pipeline(_pipeline)
    assert "1 file" in result.message
    assert "threshold=1" in result.message


def test_default_threshold_is_one(backend):
    pipeline = {"name": "no_threshold", "directory": "/out"}
    with patch("pipewatch.backends.ftp.ftplib.FTP") as MockFTP:
        MockFTP.return_value = _make_ftp_mock(["/out/x.csv"])
        result = backend.check_pipeline(pipeline)
    assert result.status == PipelineStatus.HEALTHY
