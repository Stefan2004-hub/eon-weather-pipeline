"""Command-line interface for the local weather pipeline."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from pydantic import ValidationError

from weather_pipeline.config import Settings
from weather_pipeline.orchestration import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    """Build the public command-line parser."""

    parser = argparse.ArgumentParser(description="Run the E.ON weather intelligence pipeline.")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("run", help="Run Bronze, Silver, Gold, and reporting stages.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return a shell-compatible process exit code."""

    parser = build_parser()
    arguments = parser.parse_args(argv)
    if arguments.command != "run":
        parser.error(f"unsupported command: {arguments.command}")

    try:
        run_pipeline(Settings())
    except ValidationError as exc:
        logging.basicConfig(level=logging.ERROR, format="%(levelname)s %(message)s")
        logging.getLogger(__name__).error("invalid configuration: %s", exc)
        return 2
    except Exception:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
