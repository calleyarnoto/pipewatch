"""Register the NATS backend with the pipewatch backend registry."""

from pipewatch.backends import register_backend
from pipewatch.backends.nats import NATSBackend

register_backend("nats", NATSBackend)
