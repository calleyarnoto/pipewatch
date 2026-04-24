"""Register the CouchDB backend with the pipewatch backend registry."""

from pipewatch.backends import register_backend
from pipewatch.backends.couchdb import CouchDBBackend

register_backend("couchdb", CouchDBBackend)
