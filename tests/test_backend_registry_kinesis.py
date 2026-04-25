"""Verify the Kinesis backend is correctly registered."""
from pipewatch.backends import get_backend_class
import pipewatch.backends.kinesis_register  # noqa: F401 — side-effect import


def test_kinesis_backend_is_registered():
    cls = get_backend_class("kinesis")
    from pipewatch.backends.kinesis import KinesisBackend
    assert cls is KinesisBackend


def test_kinesis_backend_name_case_insensitive():
    cls_lower = get_backend_class("kinesis")
    cls_upper = get_backend_class("KINESIS")
    cls_mixed = get_backend_class("Kinesis")
    assert cls_lower is cls_upper is cls_mixed
