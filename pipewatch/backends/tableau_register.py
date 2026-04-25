"""Register the Tableau backend with the pipewatch backend registry."""

from pipewatch.backends import register_backend
from pipewatch.backends.tableau import TableauBackend

register_backend("tableau", TableauBackend)
