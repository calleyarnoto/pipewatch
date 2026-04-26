"""Integration-style tests confirming Spark wires up end-to-end via the
built-in registry and produces correctly shaped PipelineResult objects."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import importlib

from pipewatch.backends import get_backend_class
from pipewatch.backends.base import PipelineStatus
from pipewatch.config import PipelineConfig


def _ensure_registered():
    importlib.import_module("pipewatch.backends.spark_register")


def test_spark_in_builtins():
    _ensure_registered()
    cls = get_backend_class("spark")
    assert cls is not None


def test_spark_result_has_pipeline_name():
    _ensure_registered()
    SparkBackend = get_backend_class("spark")
    backend = SparkBackend({"history_server": "http://localhost:18080"})
    pipeline = PipelineConfig(name="smoke_test", extras={"app_name": "smoke_test"})

    payload = [
        {
            "name": "smoke_test",
            "attempts": [{"completed": True, "duration": 8000}],
        }
    ]
    mock_resp = MagicMock()
    mock_resp.json.return_value = payload
    mock_resp.raise_for_status.return_value = None

    with patch("pipewatch.backends.spark.requests.get", return_value=mock_resp):
        result = backend.check_pipeline(pipeline)

    assert result.pipeline_name == "smoke_test"
    assert result.status == PipelineStatus.HEALTHY
