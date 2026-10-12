"""Typed values used by the production ingestion workflow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Literal

DatasetType = Literal["forecast", "historical"]
HOURLY_VARIABLES = ("temperature_2m", "wind_speed_10m", "direct_radiation")


@dataclass(frozen=True, slots=True)
class Location:
    """A configured weather extraction location."""

    id: str
    name: str
    latitude: float
    longitude: float
    timezone: str


@dataclass(frozen=True, slots=True)
class IngestionRequest:
    """A validated request for one location and dataset."""

    dataset: DatasetType
    location: Location
    forecast_days: int | None = None
    start_date: date | None = None
    end_date: date | None = None


@dataclass(frozen=True, slots=True)
class ExtractionResult:
    """The persisted outcome of one location extraction."""

    extraction_id: str
    location_id: str
    dataset: DatasetType
    status: Literal["SUCCESS", "FAILED"]
    bronze_path: Path | None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class IngestionRunSummary:
    """The aggregate outcome returned by a multi-location ingestion run."""

    run_id: str
    started_at: datetime
    completed_at: datetime
    results: tuple[ExtractionResult, ...]

    @property
    def successful_count(self) -> int:
        return sum(result.status == "SUCCESS" for result in self.results)

    @property
    def failed_count(self) -> int:
        return sum(result.status == "FAILED" for result in self.results)

    @property
    def status(self) -> str:
        if self.failed_count == 0:
            return "COMPLETED"
        if self.successful_count:
            return "PARTIAL"
        return "FAILED"
