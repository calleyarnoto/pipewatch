"""Register the Kinesis backend with the pipewatch backend registry."""
from pipewatch.backends import register_backend
from pipewatch.backends.kinesis import KinesisBackend

register_backend("kinesis", KinesisBackend)
