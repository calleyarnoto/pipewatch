"""Ensure TimescaleDB is wired into the built-in backend registry."""

from pipewatch.backends import _register_builtins, get_backend_class
from pipewatch.backends.timescaledb import TimescaleDBBackend


def test_timescaledb_in_builtins(monkeypatch):
    """After calling _register_builtins the timescaledb key must resolve."""
    # Re-run registration to simulate a fresh interpreter state.
    _register_builtins()
    cls = get_backend_class("timescaledb")
    assert cls is TimescaleDBBackend


def test_timescaledb_result_has_pipeline_name():
    """Smoke-test that the backend returns a result carrying the pipeline name."""
    from types import SimpleNamespace
    from unittest.mock import MagicMock, patch

    backend = TimescaleDBBackend(
        {"host": "h", "port": 5432, "dbname": "d", "user": "u", "password": "p"}
    )
    pipeline = SimpleNamespace(name="my_ts_pipe", params={"query": "SELECT 5", "threshold": 1})

    cursor = MagicMock()
    cursor.__enter__ = lambda s: s
    cursor.__exit__ = MagicMock(return_value=False)
    cursor.fetchone.return_value = (5,)
    conn = MagicMock()
    conn.cursor.return_value = cursor

    with patch("psycopg2.connect", return_value=conn):
        result = backend.check_pipeline(pipeline)

    assert result.name == "my_ts_pipe"
