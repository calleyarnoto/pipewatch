"""Register the OpenSearch backend with the pipewatch backend registry.

This module is imported as a side-effect during registry initialisation so that
``opensearch`` becomes available as a first-class backend name without requiring
users to install any extra entry-points.
"""
from __future__ import annotations

from pipewatch.backends import register_backend
from pipewatch.backends.opensearch import OpenSearchBackend

register_backend("opensearch", OpenSearchBackend)
