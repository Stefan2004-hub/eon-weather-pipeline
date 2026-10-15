"""Multi-location Open-Meteo ingestion and immutable Bronze persistence."""

from __future__ import annotations

import hashlib
from datetime import UTC, date, datetime
from pathlib import Path
from time import monotonic
from uuid import uuid4

from weather_pipeline.config import Settings
from weather_pipeline.ingestion.client import OpenMeteoClient, OpenMeteoClientError
from weather_pipeline.ingestion.locations import LocationConfigurationError, validate_location
from weather_pipeline.ingestion.models import (
    HOURLY_VARIABLES,
    DatasetType,
    ExtractionResult,
    IngestionRequest,
    IngestionRunSummary,
    Location,
)
from weather_pipeline.logging import ContextAdapter, with_context
from weather_pipeline.storage.local import (
    append_json_line,
    write_bytes_atomically,
    write_json_atomically,
)


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
    _validate_locations(locations)
    owns_client = client is None
    resolved_client = client or OpenMeteoClient(
        connect_timeout_seconds=settings.connect_timeout_seconds,
        read_timeout_seconds=settings.read_timeout_seconds,
        max_retries=settings.max_retries,
        retry_backoff_seconds=settings.retry_backoff_seconds,
        max_retry_after_seconds=settings.max_retry_after_seconds,
    )
    started_at = datetime.now(UTC)
    run_id = uuid4().hex
    run_logger = with_context(logger, run_id=run_id, stage="bronze")
    results: list[ExtractionResult] = []
    try:
        for location in locations:
            request = IngestionRequest(dataset, location, forecast_days, start_date, end_date)
            results.append(_extract_one(settings, request, run_id, resolved_client, run_logger))
        return IngestionRunSummary(run_id, started_at, datetime.now(UTC), tuple(results))
    finally:
        if owns_client:
            resolved_client.close()


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
        "data_classification": (
            "modeled_forecast" if request.dataset == "forecast" else "modeled_reanalysis"
        ),
        "request_parameters": parameters,
        "retrieved_at_utc": retrieved_at.isoformat(),
        "valid_time_encoding": "unix_seconds_utc",
        "location_timezone": request.location.timezone,
        "forecast_issue_time_utc": None,
        "forecast_issue_time_reason": "not exposed reliably by current Open-Meteo forecast API",
    }
    response = None
    try:
        logger.info(
            "requesting Open-Meteo data",
            extra={"location": request.location.id, "dataset": request.dataset},
        )
        response = client.get_json(endpoint, parameters)
        metadata.update(
            {
                "request_duration_ms": response.duration_ms,
                "request_attempts": response.attempts,
                "http_status": response.status_code,
            }
        )
        _validate_payload(response.payload)
        bronze_path = _bronze_path(settings.bronze_dir, request, retrieved_at, extraction_id)
        write_bytes_atomically(bronze_path, response.body)
        metadata.update(
            {
                "response_checksum_sha256": hashlib.sha256(response.body).hexdigest(),
                "bronze_file_path": str(bronze_path),
                "response_units": response.payload["hourly_units"],
                "status": "SUCCESS",
            }
        )
        metadata_path = _metadata_path(bronze_path)
        write_json_atomically(metadata_path, metadata)
        try:
            append_json_line(settings.manifest_path, metadata)
        except OSError as exc:
            logger.error(
                "Bronze payload is not indexed in manifest",
                extra={"location": request.location.id, "path": bronze_path, "error": str(exc)},
            )
            return ExtractionResult(
                extraction_id,
                request.location.id,
                request.dataset,
                "FAILED_UNTRACEABLE",
                bronze_path,
                f"manifest persistence failed: {exc}",
                metadata_path,
            )
        logger.info(
            "saved raw payload", extra={"location": request.location.id, "path": bronze_path}
        )
        return ExtractionResult(
            extraction_id,
            request.location.id,
            request.dataset,
            "SUCCESS",
            bronze_path,
            None,
            metadata_path,
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
        if isinstance(exc, OpenMeteoClientError):
            if exc.status_code is not None:
                metadata["http_status"] = exc.status_code
            if exc.duration_ms is not None:
                metadata["request_duration_ms"] = exc.duration_ms
        diagnostic_body = response.body if response is not None else getattr(exc, "body", None)
        if isinstance(diagnostic_body, bytes):
            diagnostic_path = _diagnostic_path(
                settings.bronze_dir, request, retrieved_at, extraction_id
            )
            try:
                write_bytes_atomically(diagnostic_path, diagnostic_body)
                metadata["diagnostic_response_path"] = str(diagnostic_path)
                metadata["diagnostic_response_checksum_sha256"] = hashlib.sha256(
                    diagnostic_body
                ).hexdigest()
            except OSError as diagnostic_exc:
                metadata["diagnostic_persistence_error"] = str(diagnostic_exc)
        try:
            append_json_line(settings.manifest_path, metadata)
        except OSError as manifest_exc:
            logger.error(
                "failed extraction could not be indexed in manifest",
                extra={"location": request.location.id, "error": str(manifest_exc)},
            )
            return ExtractionResult(
                extraction_id,
                request.location.id,
                request.dataset,
                "FAILED_UNTRACEABLE",
                None,
                f"{exc}; manifest persistence failed: {manifest_exc}",
            )
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
        "timezone": "GMT",
        "temperature_unit": "celsius",
        "wind_speed_unit": "ms",
        "timeformat": "unixtime",
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
    if end_date > datetime.now(UTC).date():
        raise IngestionValidationError("historical dates must not be in the future")
    if start_date < date(1940, 1, 1):
        raise IngestionValidationError("historical dates must not precede 1940-01-01")


def _validate_payload(payload: dict[str, object]) -> None:
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict):
        raise IngestionValidationError("Open-Meteo response is missing an hourly object")
    required = ("time", *HOURLY_VARIABLES)
    if any(not isinstance(hourly.get(field), list) for field in required):
        raise IngestionValidationError("Open-Meteo response is missing required hourly arrays")
    lengths = {len(hourly[field]) for field in required}
    if len(lengths) != 1:
        raise IngestionValidationError(
            "Open-Meteo response hourly arrays have inconsistent lengths"
        )
    if any(not isinstance(value, int) for value in hourly["time"]):
        raise IngestionValidationError("Open-Meteo response time values must be Unix seconds")
    units = payload.get("hourly_units")
    expected_units = {
        "temperature_2m": "°C",
        "wind_speed_10m": "m/s",
        "direct_radiation": "W/m²",
    }
    if not isinstance(units, dict) or any(
        units.get(key) != value for key, value in expected_units.items()
    ):
        raise IngestionValidationError(
            "Open-Meteo response units do not match the ingestion contract"
        )


def _validate_locations(locations: tuple[Location, ...]) -> None:
    ids: set[str] = set()
    for location in locations:
        try:
            validate_location(location, ids)
        except LocationConfigurationError as exc:
            raise IngestionValidationError(str(exc)) from exc
        ids.add(location.id)


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


def _metadata_path(bronze_path: Path) -> Path:
    return bronze_path.with_suffix(".metadata.json")


def _diagnostic_path(
    bronze_dir: Path, request: IngestionRequest, retrieved_at: datetime, extraction_id: str
) -> Path:
    return (
        bronze_dir
        / "source=open_meteo"
        / f"dataset={request.dataset}"
        / f"location={request.location.id}"
        / f"ingestion_date={retrieved_at.date().isoformat()}"
        / "failed"
        / f"{extraction_id}.response"
    )
