"""Open-Meteo forecast ingestion for the existing single-location pipeline."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from weather_pipeline.config import Settings
from weather_pipeline.logging import ContextAdapter, with_context
from weather_pipeline.storage import write_json


class IngestionError(RuntimeError):
    """Raised when the Open-Meteo response cannot be retrieved or decoded."""


def extract_weather(settings: Settings, logger: ContextAdapter) -> Path:
    """Fetch one forecast response and save its unmodified payload as Bronze JSON."""

    stage_logger = with_context(logger, stage="bronze")
    parameters = {
        "latitude": settings.latitude,
        "longitude": settings.longitude,
        "hourly": "temperature_2m,wind_speed_10m,direct_radiation",
        "timezone": settings.timezone,
    }
    stage_logger.info("requesting Open-Meteo forecast", extra={"url": settings.api_base_url})
    try:
        response = requests.get(settings.api_base_url, params=parameters, timeout=30)
        response.raise_for_status()
        payload: dict[str, Any] = response.json()
    except requests.RequestException as exc:
        raise IngestionError(f"Open-Meteo request failed: {exc}") from exc
    except ValueError as exc:
        raise IngestionError("Open-Meteo response was not valid JSON") from exc

    if not isinstance(payload, dict):
        raise IngestionError("Open-Meteo response must be a JSON object")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bronze_path = settings.bronze_dir / f"weather_raw_{timestamp}.json"
    write_json(bronze_path, payload)
    stage_logger.info("saved raw payload", extra={"path": bronze_path})
    return bronze_path
