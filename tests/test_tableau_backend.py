"""Tests for pipewatch.backends.tableau.TableauBackend."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from pipewatch.backends.tableau import TableauBackend
from pipewatch.backends.base import PipelineStatus


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def backend():
    return TableauBackend(
        {
            "server_url": "https://tableau.example.com",
            "token_name": "mytoken",
            "token_value": "secret",
            "site_id": "mysite",
        }
    )


@pytest.fixture()
def _pipeline():
    return SimpleNamespace(
        name="orders_extract",
        config={"datasource_id": "abc-123", "max_age_hours": 12},
    )


def _sign_in_mock(token="tok", site_luid="site-luid"):
    """Return a mock that replaces _sign_in on the backend instance."""
    return MagicMock(return_value=(token, site_luid))


def _datasource_response(updated_at: str):
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"datasource": {"updatedAt": updated_at}}
    return mock_resp


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_healthy_when_refreshed_within_threshold(backend, _pipeline):
    recent = (datetime.now(timezone.utc) - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    backend._sign_in = _sign_in_mock()
    backend._session.get = MagicMock(return_value=_datasource_response(recent))

    result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.HEALTHY
    assert result.pipeline_name == "orders_extract"


def test_failed_when_refresh_exceeds_threshold(backend, _pipeline):
    stale = (datetime.now(timezone.utc) - timedelta(hours=20)).strftime("%Y-%m-%dT%H:%M:%SZ")
    backend._sign_in = _sign_in_mock()
    backend._session.get = MagicMock(return_value=_datasource_response(stale))

    result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.FAILED
    assert "threshold" in result.message


def test_unknown_when_datasource_id_missing(backend):
    pipeline = SimpleNamespace(name="no_id_pipeline", config={})
    result = backend.check_pipeline(pipeline)

    assert result.status == PipelineStatus.UNKNOWN
    assert "datasource_id" in result.message


def test_unknown_when_updated_at_missing(backend, _pipeline):
    backend._sign_in = _sign_in_mock()
    mock_resp = MagicMock()
    mock_resp.raise_for_status = MagicMock()
    mock_resp.json.return_value = {"datasource": {}}
    backend._session.get = MagicMock(return_value=mock_resp)

    result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.UNKNOWN
    assert "updatedAt" in result.message


def test_unknown_on_request_exception(backend, _pipeline):
    backend._sign_in = MagicMock(side_effect=RuntimeError("connection refused"))

    result = backend.check_pipeline(_pipeline)

    assert result.status == PipelineStatus.UNKNOWN
    assert "connection refused" in result.message


def test_default_max_age_hours_applied(backend):
    """When max_age_hours is absent the default of 24 h is used."""
    pipeline = SimpleNamespace(name="default_age", config={"datasource_id": "xyz"})
    # refreshed 30 hours ago — should fail with default 24 h threshold
    stale = (datetime.now(timezone.utc) - timedelta(hours=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    backend._sign_in = _sign_in_mock()
    backend._session.get = MagicMock(return_value=_datasource_response(stale))

    result = backend.check_pipeline(pipeline)

    assert result.status == PipelineStatus.FAILED


def test_tableau_backend_is_registered():
    import pipewatch.backends.tableau_register  # noqa: F401 — side-effect import
    from pipewatch.backends import get_backend_class

    cls = get_backend_class("tableau")
    assert cls is TableauBackend


def test_tableau_backend_name_case_insensitive():
    import pipewatch.backends.tableau_register  # noqa: F401
    from pipewatch.backends import get_backend_class

    assert get_backend_class("Tableau") is TableauBackend
    assert get_backend_class("TABLEAU") is TableauBackend
