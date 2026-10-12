"""Loading and validation of configured extraction locations."""

from __future__ import annotations

from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml

from weather_pipeline.ingestion.models import Location


class LocationConfigurationError(ValueError):
    """Raised when the locations catalogue is malformed."""


def load_locations(path: Path) -> tuple[Location, ...]:
    """Load a non-empty, uniquely identified location catalogue from YAML."""

    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise LocationConfigurationError(f"could not read locations file {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise LocationConfigurationError(f"locations file {path} is not valid YAML") from exc

    entries = raw.get("locations") if isinstance(raw, dict) else None
    if not isinstance(entries, list) or not entries:
        raise LocationConfigurationError("locations file must contain a non-empty 'locations' list")

    locations: list[Location] = []
    ids: set[str] = set()
    for index, entry in enumerate(entries, start=1):
        if not isinstance(entry, dict):
            raise LocationConfigurationError(f"location #{index} must be a mapping")
        try:
            location = Location(
                id=str(entry["id"]),
                name=str(entry["name"]),
                latitude=float(entry["latitude"]),
                longitude=float(entry["longitude"]),
                timezone=str(entry["timezone"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise LocationConfigurationError(
                f"location #{index} has invalid required fields"
            ) from exc
        is_valid_id = location.id and all(
            char.islower() or char.isdigit() or char == "-" for char in location.id
        )
        if not is_valid_id:
            raise LocationConfigurationError(f"location #{index} has invalid id {location.id!r}")
        if location.id in ids:
            raise LocationConfigurationError(f"duplicate location id {location.id!r}")
        if not -90 <= location.latitude <= 90 or not -180 <= location.longitude <= 180:
            raise LocationConfigurationError(
                f"location {location.id!r} has out-of-range coordinates"
            )
        try:
            ZoneInfo(location.timezone)
        except ZoneInfoNotFoundError as exc:
            raise LocationConfigurationError(
                f"location {location.id!r} has invalid timezone"
            ) from exc
        ids.add(location.id)
        locations.append(location)
    return tuple(locations)
