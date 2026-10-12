"""Command-line interface for the local weather pipeline."""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence
from datetime import date
from uuid import uuid4

from pydantic import ValidationError

from weather_pipeline.config import Settings
from weather_pipeline.ingestion import run_ingestion
from weather_pipeline.ingestion.locations import LocationConfigurationError, load_locations
from weather_pipeline.ingestion.service import IngestionValidationError
from weather_pipeline.logging import configure_logging, get_logger
from weather_pipeline.orchestration import run_pipeline


def build_parser() -> argparse.ArgumentParser:
    """Build the public command-line parser."""

    parser = argparse.ArgumentParser(description="Run the E.ON weather intelligence pipeline.")
    subcommands = parser.add_subparsers(dest="command", required=True)
    subcommands.add_parser("run", help="Run Bronze, Silver, Gold, and reporting stages.")
    ingest_parser = subcommands.add_parser(
        "ingest", help="Extract immutable raw forecast or historical Bronze data."
    )
    ingest_parser.add_argument("--dataset", choices=("forecast", "historical"), default="forecast")
    ingest_parser.add_argument(
        "--location",
        action="append",
        dest="location_ids",
        metavar="ID",
        help="Location ID; repeatable.",
    )
    ingest_parser.add_argument("--start-date", type=_parse_date, metavar="YYYY-MM-DD")
    ingest_parser.add_argument("--end-date", type=_parse_date, metavar="YYYY-MM-DD")
    ingest_parser.add_argument("--forecast-days", type=int, metavar="DAYS")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the CLI and return a shell-compatible process exit code."""

    parser = build_parser()
    arguments = parser.parse_args(argv)
    try:
        settings = Settings()
    except ValidationError as exc:
        logging.basicConfig(level=logging.ERROR, format="%(levelname)s %(message)s")
        logging.getLogger(__name__).error("invalid configuration: %s", exc)
        return 2
    if arguments.command == "run":
        try:
            run_pipeline(settings)
        except Exception:
            return 1
        return 0
    if arguments.command == "ingest":
        return _run_ingest(settings, arguments)
    parser.error(f"unsupported command: {arguments.command}")
    return 2


def _run_ingest(settings: Settings, arguments: argparse.Namespace) -> int:
    """Run the Phase 2 raw-ingestion command and render its concise outcome."""

    try:
        locations = load_locations(settings.locations_file)
        if arguments.location_ids:
            requested_ids = set(arguments.location_ids)
            known_ids = {location.id for location in locations}
            unknown_ids = requested_ids - known_ids
            if unknown_ids:
                raise IngestionValidationError(
                    f"unknown location IDs: {', '.join(sorted(unknown_ids))}"
                )
            locations = tuple(location for location in locations if location.id in requested_ids)
        configure_logging(settings.log_level)
        logger = get_logger(__name__, uuid4().hex)
        summary = run_ingestion(
            settings,
            locations,
            dataset=arguments.dataset,
            forecast_days=(settings.forecast_days if arguments.dataset == "forecast" else None)
            if arguments.forecast_days is None
            else arguments.forecast_days,
            start_date=arguments.start_date,
            end_date=arguments.end_date,
            logger=logger,
        )
    except (IngestionValidationError, LocationConfigurationError) as exc:
        logging.basicConfig(level=logging.ERROR, format="%(levelname)s %(message)s")
        logging.getLogger(__name__).error("invalid ingestion request: %s", exc)
        return 2
    except Exception:
        logging.getLogger(__name__).exception("ingestion setup failed")
        return 1

    failed_locations = [
        result.location_id for result in summary.results if result.status == "FAILED"
    ]
    logging.getLogger(__name__).info(
        "ingestion %s: %s succeeded, %s failed%s",
        summary.status,
        summary.successful_count,
        summary.failed_count,
        f" (failed: {', '.join(failed_locations)})" if failed_locations else "",
    )
    return 0 if summary.status == "COMPLETED" else 1


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("must use YYYY-MM-DD") from exc


if __name__ == "__main__":
    raise SystemExit(main())
