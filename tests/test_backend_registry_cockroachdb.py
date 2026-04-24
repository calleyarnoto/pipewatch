import pytest
from pipewatch.backends import get_backend_class
import pipewatch.backends.cockroachdb_register  # noqa: F401


def test_cockroachdb_backend_is_registered():
    cls = get_backend_class("cockroachdb")
    from pipewatch.backends.cockroachdb import CockroachDBBackend
    assert cls is CockroachDBBackend


def test_cockroachdb_backend_name_case_insensitive():
    cls_lower = get_backend_class("cockroachdb")
    cls_upper = get_backend_class("CockroachDB")
    assert cls_lower is cls_upper
