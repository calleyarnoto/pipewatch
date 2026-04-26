"""Register the Spark backend with the pipewatch backend registry."""

from pipewatch.backends import register_backend
from pipewatch.backends.spark import SparkBackend

register_backend("spark", SparkBackend)
