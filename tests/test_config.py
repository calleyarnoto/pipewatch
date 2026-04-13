"""Tests for pipewatch configuration loading."""

import os
import textwrap

import pytest

from pipewatch.config import AppConfig, PipelineConfig, load_config


@pytest.fixture
def config_file(tmp_path):
    """Write a sample config file and return its path."""
    content = textwrap.dedent("""\
        backend: sqlite
        backend_options:
          db_path: /tmp/pipewatch.db
        alert_channels:
          - slack
        pipelines:
          - name: daily_sales
            schedule: "0 6 * * *"
            max_duration_seconds: 3600
            alert_on_failure: true
            tags:
              - finance
              - daily
          - name: user_sync
            alert_on_failure: false
    """)
    cfg = tmp_path / "pipewatch.yml"
    cfg.write_text(content)
    return str(cfg)


def test_load_config_returns_app_config(config_file):
    cfg = load_config(config_file)
    assert isinstance(cfg, AppConfig)


def test_load_config_backend(config_file):
    cfg = load_config(config_file)
    assert cfg.backend == "sqlite"
    assert cfg.backend_options == {"db_path": "/tmp/pipewatch.db"}


def test_load_config_alert_channels(config_file):
    cfg = load_config(config_file)
    assert cfg.alert_channels == ["slack"]


def test_load_config_pipelines(config_file):
    cfg = load_config(config_file)
    assert len(cfg.pipelines) == 2

    daily = cfg.pipelines[0]
    assert isinstance(daily, PipelineConfig)
    assert daily.name == "daily_sales"
    assert daily.schedule == "0 6 * * *"
    assert daily.max_duration_seconds == 3600
    assert daily.alert_on_failure is True
    assert "finance" in daily.tags

    sync = cfg.pipelines[1]
    assert sync.name == "user_sync"
    assert sync.alert_on_failure is False
    assert sync.schedule is None


def test_load_config_file_not_found():
    with pytest.raises(FileNotFoundError, match="No pipewatch config file found"):
        load_config("/nonexistent/path/pipewatch.yml")


def test_load_config_malformed(tmp_path):
    bad = tmp_path / "bad.yml"
    bad.write_text("- just\n- a\n- list\n")
    with pytest.raises(ValueError, match="must contain a YAML mapping"):
        load_config(str(bad))


def test_load_config_defaults(tmp_path):
    minimal = tmp_path / "pipewatch.yml"
    minimal.write_text("pipelines: []\n")
    cfg = load_config(str(minimal))
    assert cfg.backend == "sqlite"
    assert cfg.pipelines == []
    assert cfg.alert_channels == []
