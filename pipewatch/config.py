"""Configuration loading and validation for pipewatch."""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml

DEFAULT_CONFIG_PATHS = [
    "pipewatch.yml",
    "pipewatch.yaml",
    ".pipewatch.yml",
    os.path.expanduser("~/.config/pipewatch/config.yml"),
]


@dataclass
class PipelineConfig:
    name: str
    schedule: Optional[str] = None
    max_duration_seconds: Optional[int] = None
    alert_on_failure: bool = True
    tags: List[str] = field(default_factory=list)
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class AppConfig:
    pipelines: List[PipelineConfig] = field(default_factory=list)
    backend: str = "sqlite"
    backend_options: Dict[str, Any] = field(default_factory=dict)
    alert_channels: List[str] = field(default_factory=list)


def load_config(path: Optional[str] = None) -> AppConfig:
    """Load configuration from a YAML file.

    Args:
        path: Explicit path to config file. If None, searches default locations.

    Returns:
        Parsed AppConfig instance.

    Raises:
        FileNotFoundError: If no config file is found.
        ValueError: If the config file is malformed.
    """
    config_path = path or _find_config_file()
    if config_path is None:
        raise FileNotFoundError(
            "No pipewatch config file found. "
            "Create a pipewatch.yml or pass --config explicitly."
        )

    with open(config_path, "r") as f:
        raw = yaml.safe_load(f)

    if not isinstance(raw, dict):
        raise ValueError(f"Config file {config_path} must contain a YAML mapping.")

    pipelines = [
        PipelineConfig(
            name=p["name"],
            schedule=p.get("schedule"),
            max_duration_seconds=p.get("max_duration_seconds"),
            alert_on_failure=p.get("alert_on_failure", True),
            tags=p.get("tags", []),
            extra={k: v for k, v in p.items() if k not in {
                "name", "schedule", "max_duration_seconds", "alert_on_failure", "tags"
            }},
        )
        for p in raw.get("pipelines", [])
    ]

    return AppConfig(
        pipelines=pipelines,
        backend=raw.get("backend", "sqlite"),
        backend_options=raw.get("backend_options", {}),
        alert_channels=raw.get("alert_channels", []),
    )


def _find_config_file() -> Optional[str]:
    """Search default locations for a config file."""
    for candidate in DEFAULT_CONFIG_PATHS:
        if os.path.isfile(candidate):
            return candidate
    return None
