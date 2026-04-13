"""Command-line interface for pipewatch.

Provides the main entry point and CLI commands for running pipeline
health checks and displaying results.
"""

import sys
import argparse
import logging
from pathlib import Path
from typing import Optional

from pipewatch.config import load_config, AppConfig
from pipewatch.backends.dummy import DummyBackend
from pipewatch.alerts import LogAlertChannel
from pipewatch.runner import RunReport, run_checks

logger = logging.getLogger(__name__)


def _setup_logging(verbose: bool = False) -> None:
    """Configure logging based on verbosity flag."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        level=level,
    )


def _resolve_backend(config: AppConfig):
    """Instantiate the backend specified in config.

    Currently supports 'dummy'. Additional backends can be registered here
    as the project grows.
    """
    backend_name = config.backend.lower()
    if backend_name == "dummy":
        return DummyBackend()
    else:
        raise ValueError(
            f"Unknown backend '{backend_name}'. "
            "Supported backends: dummy"
        )


def _resolve_alert_channels(config: AppConfig) -> list:
    """Instantiate alert channels listed in config."""
    channels = []
    for channel_name in config.alert_channels:
        name = channel_name.lower()
        if name == "log":
            channels.append(LogAlertChannel())
        else:
            logger.warning("Unknown alert channel '%s' — skipping.", channel_name)
    return channels


def run_command(config_path: Optional[str], verbose: bool) -> int:
    """Execute the 'run' command: check all pipelines and report results.

    Returns an exit code (0 = all healthy, 1 = failures detected).
    """
    _setup_logging(verbose)

    try:
        config = load_config(config_path)
    except FileNotFoundError as exc:
        logger.error("Config file not found: %s", exc)
        return 2
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to load config: %s", exc)
        return 2

    logger.debug("Loaded config with %d pipeline(s).", len(config.pipelines))

    try:
        backend = _resolve_backend(config)
    except ValueError as exc:
        logger.error("%s", exc)
        return 2

    alert_channels = _resolve_alert_channels(config)

    report: RunReport = run_checks(
        pipelines=config.pipelines,
        backend=backend,
        alert_channels=alert_channels,
    )

    print(report.summary())

    return 0 if report.unhealthy == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser."""
    parser = argparse.ArgumentParser(
        prog="pipewatch",
 and alert on ETL pipeline health.",
    )
    parser.add_argument(
        "-c", "--config",
        metavar="FILE",
        default=None,
        help="Path to pipewatch config file (default: auto-detect).",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        default=False,
        help="Enable debug logging.",
    )

    subparsers = parser.add_subparsers(dest="command")

    # 'run' subcommand
    subparsers.add_parser(
        "run",
        help="Run pipeline health checks and report results.",
    )

    return parser


def main(argv: Optional[list] = None) -> int:
    """Main entry point for the pipewatch CLI."""
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "run" or args.command is None:
        # Default to 'run' if no subcommand given
        return run_command(config_path=args.config, verbose=args.verbose)

    parser.print_help()
    return 0


if __name__ == "__main__":
    sys.exit(main())
