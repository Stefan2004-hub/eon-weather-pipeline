"""Multi-location Open-Meteo ingestion and immutable Bronze persistence."""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path
from time import monotonic
from uuid import uuid4

from weather_pipeline.config import Settings
from weather_pipeline.ingestion.client import OpenMeteoClient, OpenMeteoClientError
from weather_pipeline.ingestion.models import (
    HOURLY_VARIABLES,
    DatasetType,
    ExtractionResult,
    IngestionRequest,
    IngestionRunSummary,
    Location,
)
from weather_pipeline.logging import ContextAdapter, with_context
from weather_pipeline.storage.local import append_json_line, write_bytes_atomically


class IngestionValidationError(ValueError):
    """Raised before an invalid ingestion request is sent to the provider."""


def run_ingestion(
    settings: Settings,
    locations: tuple[Location, ...],
    *,
    dataset: DatasetType,
    forecast_days: int | None,
    start_date: date | None,
    end_date: date | None,
    logger: ContextAdapter,
    client: OpenMeteoClient | None = None,
) -> IngestionRunSummary:
    """Extract each location independently and write all attempt metadata."""

    _validate_request(dataset, forecast_days, start_date, end_date)
    resolved_client = client or OpenMeteoClient(
        connect_timeout_seconds=settings.connect_timeout_seconds,
        read_timeout_seconds=settings.read_timeout_seconds,
        max_retries=settings.max_retries,
        retry_backoff_seconds=settings.retry_backoff_seconds,
    )
    started_at = datetime.now(UTC)
    run_id = uuid4().hex
    run_logger = with_context(logger, run_id=run_id, stage="bronze")
    results: list[ExtractionResult] = []
    for location in locations:
        request = IngestionRequest(dataset, location, forecast_days, start_date, end_date)
        results.append(_extract_one(settings, request, run_id, resolved_client, run_logger))
    return IngestionRunSummary(run_id, started_at, datetime.now(UTC), tuple(results))


def _extract_one(
    settings: Settings,
    request: IngestionRequest,
    run_id: str,
    client: OpenMeteoClient,
    logger: ContextAdapter,
) -> ExtractionResult:
    extraction_id = uuid4().hex
    endpoint = (
        settings.api_base_url if request.dataset == "forecast" else settings.archive_api_base_url
    )
    parameters = _parameters(request)
    retrieved_at = datetime.now(UTC)
    started = monotonic()
    metadata: dict[str, object] = {
        "run_id": run_id,
        "extraction_id": extraction_id,
        "api_provider": "open_meteo",
        "endpoint": endpoint,
        "location_id": request.location.id,
        "location_name": request.location.name,
        "dataset": request.dataset,
        "request_parameters": parameters,
        "retrieved_at_utc": retrieved_at.isoformat(),
    }
    try:
        logger.info(
            "requesting Open-Meteo data",
            extra={"location": request.location.id, "dataset": request.dataset},
        )
        response = client.get_json(endpoint, parameters)
        _validate_payload(response.payload)
        bronze_path = _bronze_path(settings.bronze_dir, request, retrieved_at, extraction_id)
        write_bytes_atomically(bronze_path, response.body)
        metadata.update(
            {
                "request_duration_ms": response.duration_ms,
                "http_status": response.status_code,
                "response_checksum_sha256": hashlib.sha256(response.body).hexdigest(),
                "bronze_file_path": str(bronze_path),
                "status": "SUCCESS",
            }
        )
        append_json_line(settings.manifest_path, metadata)
        logger.info(
            "saved raw payload", extra={"location": request.location.id, "path": bronze_path}
        )
        return ExtractionResult(
            extraction_id, request.location.id, request.dataset, "SUCCESS", bronze_path
        )
    except Exception as exc:
        metadata.update(
            {
                "request_duration_ms": round((monotonic() - started) * 1000),
                "status": "FAILED",
                "error_type": type(exc).__name__,
                "error": str(exc),
            }
        )
        if isinstance(exc, OpenMeteoClientError) and exc.status_code is not None:
            metadata["http_status"] = exc.status_code
        append_json_line(settings.manifest_path, metadata)
        logger.error(
            "Open-Meteo extraction failed",
            extra={"location": request.location.id, "error": str(exc)},
        )
        return ExtractionResult(
            extraction_id, request.location.id, request.dataset, "FAILED", None, str(exc)
        )


def _parameters(request: IngestionRequest) -> dict[str, str | float | int]:
    parameters: dict[str, str | float | int] = {
        "latitude": request.location.latitude,
        "longitude": request.location.longitude,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": request.location.timezone,
        "temperature_unit": "celsius",
        "wind_speed_unit": "ms",
        "timeformat": "iso8601",
    }
    if request.dataset == "forecast":
        parameters["forecast_days"] = request.forecast_days or 7
    else:
        assert request.start_date and request.end_date
        parameters["start_date"] = request.start_date.isoformat()
        parameters["end_date"] = request.end_date.isoformat()
    return parameters


def _validate_request(
    dataset: DatasetType, forecast_days: int | None, start_date: date | None, end_date: date | None
) -> None:
    if dataset == "forecast":
        if start_date or end_date:
            raise IngestionValidationError("forecast ingestion does not accept start or end dates")
        if forecast_days is not None and not 1 <= forecast_days <= 16:
            raise IngestionValidationError("forecast days must be between 1 and 16")
        return
    if forecast_days is not None:
        raise IngestionValidationError("historical ingestion does not accept forecast days")
    if not start_date or not end_date:
        raise IngestionValidationError("historical ingestion requires start and end dates")
    if end_date < start_date:
        raise IngestionValidationError("historical end date must not precede start date")
    if end_date > date.today():
        raise IngestionValidationError("historical dates must not be in the future")
    if (end_date - start_date).days + 1 > 31:
        raise IngestionValidationError("historical date range must not exceed 31 days")


def _validate_payload(payload: dict[str, object]) -> None:
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        raise IngestionValidationError("Open-Meteo response is missing an hourly object")
    required = ("time", *HOURLY_VARIABLES)
    if any(not isinstance(hourly.get(field), list) for field in required):
        raise IngestionValidationError("Open-Meteo response is missing required hourly arrays")


def _bronze_path(
    bronze_dir: Path, request: IngestionRequest, retrieved_at: datetime, extraction_id: str
) -> Path:
    return (
        bronze_dir
        / "source=open_meteo"
        / f"dataset={request.dataset}"
        / f"location={request.location.id}"
        / f"ingestion_date={retrieved_at.date().isoformat()}"
        / f"{extraction_id}.json"
    )
