"""Tests for pipewatch.backends.sftp.SFTPBackend."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.sftp import SFTPBackend
from pipewatch.backends.base import PipelineStatus


@pytest.fixture()
def backend() -> SFTPBackend:
    return SFTPBackend(
        {
            "host": "sftp.example.com",
            "port": 22,
            "username": "etl",
            "password": "secret",
        }
    )


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(
        name="daily_export",
        config={
            "path": "/exports/daily",
            "pattern": "*.csv",
            "threshold": 1,
        },
    )


def _make_sftp_mock(filenames):
    """Return a mock paramiko stack that lists *filenames* in any directory."""
    sftp_mock = MagicMock()
    sftp_mock.listdir.return_value = filenames

    transport_mock = MagicMock()
    sftp_from_transport = MagicMock(return_value=sftp_mock)

    return transport_mock, sftp_from_transport


def test_healthy_when_count_meets_threshold(backend, _pipeline):
    transport_mock, sftp_from = _make_sftp_mock(["a.csv", "b.csv"])
    with patch("paramiko.Transport", return_value=transport_mock), patch(
        "paramiko.SFTPClient.from_transport", sftp_from
    ):
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.HEALTHY
    assert "2" in result.detail


def test_failed_when_count_below_threshold(backend, _pipeline):
    transport_mock, sftp_from = _make_sftp_mock([])
    with patch("paramiko.Transport", return_value=transport_mock), patch(
        "paramiko.SFTPClient.from_transport", sftp_from
    ):
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.FAILED
    assert "0" in result.detail


def test_pattern_filters_files(backend):
    pipeline = SimpleNamespace(
        name="filtered",
        config={"path": "/data", "pattern": "*.parquet", "threshold": 1},
    )
    transport_mock, sftp_from = _make_sftp_mock(["file.csv", "file.parquet"])
    with patch("paramiko.Transport", return_value=transport_mock), patch(
        "paramiko.SFTPClient.from_transport", sftp_from
    ):
        result = backend.check_pipeline(pipeline)

    assert result.status == PipelineStatus.HEALTHY
    assert "1" in result.detail


def test_custom_threshold(backend):
    pipeline = SimpleNamespace(
        name="bulk",
        config={"path": "/bulk", "pattern": "*", "threshold": 5},
    )
    transport_mock, sftp_from = _make_sftp_mock(["f1", "f2", "f3"])
    with patch("paramiko.Transport", return_value=transport_mock), patch(
        "paramiko.SFTPClient.from_transport", sftp_from
    ):
        result = backend.check_pipeline(pipeline)

    assert result.status == PipelineStatus.FAILED


def test_unknown_on_connection_error(backend, _pipeline):
    with patch("paramiko.Transport", side_effect=OSError("connection refused")):
        result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.detail


def test_pipeline_name_in_result(backend, _pipeline):
    transport_mock, sftp_from = _make_sftp_mock(["x.csv"])
    with patch("paramiko.Transport", return_value=transport_mock), patch(
        "paramiko.SFTPClient.from_transport", sftp_from
    ):
        result = backend.check_pipeline(_pipeline)

    assert result.pipeline_name == "daily_export"
