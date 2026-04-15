"""Verify that CeleryBackend is registered in the backend registry."""
from __future__ import annotations

from unittest.mock import patch

import pytest

from pipewatch.backends import get_backend_class


def test_celery_backend_is_registered():
    with patch("pipewatch.backends.celery.Celery"):
        cls = get_backend_class("celery")
    from pipewatch.backends.celery import CeleryBackend

    assert cls is CeleryBackend


def test_celery_backend_name_case_insensitive():
    with patch("pipewatch.backends.celery.Celery"):
        cls_lower = get_backend_class("celery")
        cls_upper = get_backend_class("Celery")
    assert cls_lower is cls_upper
