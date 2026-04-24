from pipewatch.backends import register_backend
from pipewatch.backends.cockroachdb import CockroachDBBackend

register_backend("cockroachdb", CockroachDBBackend)
