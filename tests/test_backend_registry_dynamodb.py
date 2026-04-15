"""Verify that DynamoDB backend is registered in the backend registry."""
from __future__ import annotations

from unittest.mock import patch

import pytest

from pipewatch.backends import get_backend_class


def test_dynamodb_backend_is_registered() -> None:
    """DynamoDB backend should be retrievable by name from the registry."""
    with patch("pipewatch.backends.dynamodb.boto3"):
        cls = get_backend_class("dynamodb")
    from pipewatch.backends.dynamodb import DynamoDBBackend
    assert cls is DynamoDBBackend


def test_dynamodb_backend_name_case_insensitive() -> None:
    """Registry lookup should work regardless of case."""
    with patch("pipewatch.backends.dynamodb.boto3"):
        cls_lower = get_backend_class("dynamodb")
        cls_upper = get_backend_class("DynamoDB")
    assert cls_lower is cls_upper
