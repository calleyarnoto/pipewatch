from __future__ import annotations

from unittest.mock import patch

import pytest


def test_clickhouse_backend_is_registered():
    from pipewatch.backends import get_backend_class

    with patch.dict("sys.modules", {"clickhouse_driver": __import__("unittest.mock", fromlist=["MagicMock"]).MagicMock()}):
        cls = get_backend_class("clickhouse")
    from pipewatch.backends.clickhouse import ClickHouseBackend

    assert cls is ClickHouseBackend


def test_clickhouse_backend_name_case_insensitive():
    from pipewatch.backends import get_backend_class

    cls_lower = get_backend_class("clickhouse")
    cls_upper = get_backend_class("ClickHouse")
    assert cls_lower is cls_upper
