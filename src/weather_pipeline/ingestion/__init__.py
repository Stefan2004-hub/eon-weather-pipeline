"""Weather data ingestion implementations."""

from .open_meteo import extract_weather

__all__ = ["extract_weather"]
