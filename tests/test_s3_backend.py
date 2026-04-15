"""Tests for the S3 backend."""

from __future__ import annotations

from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.base import PipelineStatus


NOW = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture()
def backend():
    with patch("pipewatch.backends.s3.boto3") as mock_boto3:
        mock_client = MagicMock()
        mock_boto3.client.return_value = mock_client
        from pipewatch.backends.s3 import S3Backend
        b = S3Backend(region="us-east-1")
        b._s3 = MagicMock()
        yield b


def _pipeline(extras=None):
    return SimpleNamespace(
        name="test-pipeline",
        extras=extras or {"bucket": "my-bucket", "prefix": "data/"},
    )


def _make_paginator(objects):
    paginator = MagicMock()
    paginator.paginate.return_value = [{"Contents": objects}]
    return paginator


def test_healthy_when_count_meets_threshold(backend):
    objs = [{"Key": "data/file1.csv", "LastModified": NOW}]
    backend._s3.get_paginator.return_value = _make_paginator(objs)

    result = backend.check_pipeline(_pipeline())

    assert result.status == PipelineStatus.HEALTHY
    assert "1" in result.message
    assert "my-bucket" in result.message


def test_failed_when_count_below_threshold(backend):
    backend._s3.get_paginator.return_value = _make_paginator([])

    result = backend.check_pipeline(
        _pipeline({"bucket": "my-bucket", "prefix": "data/", "threshold": 1})
    )

    assert result.status == PipelineStatus.FAILED
    assert "0" in result.message


def test_custom_threshold_fails_when_not_enough_objects(backend):
    objs = [{"Key": f"data/file{i}.csv", "LastModified": NOW} for i in range(3)]
    backend._s3.get_paginator.return_value = _make_paginator(objs)

    result = backend.check_pipeline(
        _pipeline({"bucket": "b", "prefix": "p/", "threshold": 5})
    )

    assert result.status == PipelineStatus.FAILED
    assert "3" in result.message


def test_failed_when_no_objects_are_recent(backend):
    old_time = NOW - timedelta(hours=48)
    objs = [{"Key": "data/old.csv", "LastModified": old_time}]
    backend._s3.get_paginator.return_value = _make_paginator(objs)

    with patch("pipewatch.backends.s3.datetime") as mock_dt:
        mock_dt.now.return_value = NOW
        result = backend.check_pipeline(
            _pipeline({"bucket": "b", "prefix": "p/", "max_age_hours": 1})
        )

    assert result.status == PipelineStatus.FAILED
    assert "1h" in result.message or "1" in result.message


def test_unknown_when_bucket_missing(backend):
    result = backend.check_pipeline(SimpleNamespace(name="p", extras={}))

    assert result.status == PipelineStatus.UNKNOWN
    assert "bucket" in result.message


def test_unknown_on_s3_exception(backend):
    backend._s3.get_paginator.side_effect = Exception("connection refused")

    result = backend.check_pipeline(_pipeline())

    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_pipeline_name_in_result(backend):
    objs = [{"Key": "data/f.csv", "LastModified": NOW}]
    backend._s3.get_paginator.return_value = _make_paginator(objs)

    result = backend.check_pipeline(_pipeline())

    assert result.pipeline_name == "test-pipeline"
