"""Deprecated single-location compatibility wrapper for Phase 2 ingestion."""

from __future__ import annotations

from pathlib import Path

from weather_pipeline.config import Settings
from weather_pipeline.ingestion.models import Location
from weather_pipeline.ingestion.service import run_ingestion
from weather_pipeline.logging import ContextAdapter, with_context


class IngestionError(RuntimeError):
    """Raised when the Open-Meteo response cannot be retrieved or decoded."""


def extract_weather(settings: Settings, logger: ContextAdapter) -> Path:
    """Fetch the configured default location through the production ingestion engine."""

    location = Location(
        "default",
        "Configured default",
        settings.latitude,
        settings.longitude,
        settings.timezone,
    )
    summary = run_ingestion(
        settings,
        (location,),
        dataset="forecast",
        forecast_days=settings.forecast_days,
        start_date=None,
        end_date=None,
        logger=with_context(logger, stage="bronze"),
    )
    result = summary.results[0]
    if result.status != "SUCCESS" or result.bronze_path is None:
        raise IngestionError(result.error or "Open-Meteo ingestion failed")
    return result.bronze_path
