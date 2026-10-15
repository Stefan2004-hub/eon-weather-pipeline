"""Weather data ingestion implementations."""

from .open_meteo import extract_weather
from .service import run_ingestion

__all__ = ["extract_weather", "run_ingestion"]
