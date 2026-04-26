"""Verify that the Spark backend is properly registered."""

from __future__ import annotations

import importlib

import pytest

from pipewatch.backends import get_backend_class


def test_spark_backend_is_registered():
    importlib.import_module("pipewatch.backends.spark_register")
    cls = get_backend_class("spark")
    from pipewatch.backends.spark import SparkBackend
    assert cls is SparkBackend


def test_spark_backend_name_case_insensitive():
    importlib.import_module("pipewatch.backends.spark_register")
    cls_lower = get_backend_class("spark")
    cls_upper = get_backend_class("SPARK")
    assert cls_lower is cls_upper
