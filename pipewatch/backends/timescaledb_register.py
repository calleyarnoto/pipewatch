"""Register the TimescaleDB backend with the pipewatch backend registry."""

from pipewatch.backends import register_backend
from pipewatch.backends.timescaledb import TimescaleDBBackend

register_backend("timescaledb", TimescaleDBBackend)
