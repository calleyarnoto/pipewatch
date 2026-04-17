"""Registry tests for InfluxDBBackend."""
from pipewatch.backends import get_backend_class
from pipewatch.backends.influxdb import InfluxDBBackend


def test_influxdb_backend_is_registered():
    cls = get_backend_class("influxdb")
    assert cls is InfluxDBBackend


def test_influxdb_backend_name_case_insensitive():
    cls = get_backend_class("InfluxDB")
    assert cls is InfluxDBBackend
