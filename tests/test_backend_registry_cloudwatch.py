"""Verify CloudWatch backend is registered in the backend registry."""
from __future__ import annotations

from unittest.mock import patch

import pytest


def test_cloudwatch_backend_is_registered():
    with patch("pipewatch.backends.cloudwatch.boto3"):
        from pipewatch.backends import get_backend_class

        cls = get_backend_class("cloudwatch")
        from pipewatch.backends.cloudwatch import CloudWatchBackend

        assert cls is CloudWatchBackend


def test_cloudwatch_backend_name_case_insensitive():
    with patch("pipewatch.backends.cloudwatch.boto3"):
        from pipewatch.backends import get_backend_class

        assert get_backend_class("CloudWatch") is get_backend_class("cloudwatch")
